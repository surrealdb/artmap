// Copyright (c) 2026 SurrealDB Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! # SIMD Vector Search for Node16
//!
//! Provides hardware-accelerated child key matching for [`Node16`].

/// Finds the child index for `needle` within `keys[..num_children]`.
#[inline]
pub fn find_child_node16(keys: &[u8; 16], num_children: usize, needle: u8) -> Option<usize> {
    #[cfg(all(target_arch = "x86_64", target_feature = "sse2"))]
    {
        find_child_x86_sse2(keys, num_children, needle)
    }

    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    {
        find_child_neon(keys, num_children, needle)
    }

    #[cfg(not(any(
        all(target_arch = "x86_64", target_feature = "sse2"),
        all(target_arch = "aarch64", target_feature = "neon")
    )))]
    {
        find_child_scalar(keys, num_children, needle)
    }
}

/// Fallback linear search across valid key entries.
#[inline]
#[allow(dead_code)]
pub fn find_child_scalar(keys: &[u8; 16], num_children: usize, needle: u8) -> Option<usize> {
    for (i, &k) in keys[..num_children].iter().enumerate() {
        if k == needle {
            return Some(i);
        }
    }
    None
}

#[cfg(all(target_arch = "x86_64", target_feature = "sse2"))]
#[inline]
fn find_child_x86_sse2(keys: &[u8; 16], num_children: usize, needle: u8) -> Option<usize> {
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::{
        __m128i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8, _mm_set1_epi8,
    };

    unsafe {
        let needle_vec = _mm_set1_epi8(needle as i8);
        let keys_vec = _mm_loadu_si128(keys.as_ptr() as *const __m128i);
        let cmp = _mm_cmpeq_epi8(keys_vec, needle_vec);
        let mask = _mm_movemask_epi8(cmp) as u32;
        let valid_mask = (1u32 << num_children).wrapping_sub(1);
        let matches = mask & valid_mask;
        if matches != 0 {
            Some(matches.trailing_zeros() as usize)
        } else {
            None
        }
    }
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
#[inline]
fn find_child_neon(keys: &[u8; 16], num_children: usize, needle: u8) -> Option<usize> {
    #[cfg(target_arch = "aarch64")]
    use std::arch::aarch64::{
        vceqq_u8, vdupq_n_u8, vgetq_lane_u64, vld1q_u8, vreinterpretq_u64_u8,
    };

    unsafe {
        let needle_vec = vdupq_n_u8(needle);
        let keys_vec = vld1q_u8(keys.as_ptr());
        let cmp = vceqq_u8(keys_vec, needle_vec);
        // Each matching byte in `cmp` is 0xFF.
        let lo = vgetq_lane_u64(vreinterpretq_u64_u8(cmp), 0);
        let hi = vgetq_lane_u64(vreinterpretq_u64_u8(cmp), 1);

        if lo != 0 {
            let idx = (lo.trailing_zeros() >> 3) as usize;
            if idx < num_children {
                return Some(idx);
            }
        } else if hi != 0 {
            let idx = 8 + (hi.trailing_zeros() >> 3) as usize;
            if idx < num_children {
                return Some(idx);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_child_node16() {
        let mut keys = [0u8; 16];
        keys[0] = b'a';
        keys[1] = b'c';
        keys[2] = b'f';
        keys[3] = b'z';

        assert_eq!(find_child_node16(&keys, 4, b'a'), Some(0));
        assert_eq!(find_child_node16(&keys, 4, b'c'), Some(1));
        assert_eq!(find_child_node16(&keys, 4, b'f'), Some(2));
        assert_eq!(find_child_node16(&keys, 4, b'z'), Some(3));
        assert_eq!(find_child_node16(&keys, 4, b'b'), None);
        assert_eq!(find_child_node16(&keys, 4, b'm'), None);

        // Matching beyond num_children must return None
        keys[4] = b'x';
        assert_eq!(find_child_node16(&keys, 4, b'x'), None);
        assert_eq!(find_child_node16(&keys, 5, b'x'), Some(4));
    }
}
