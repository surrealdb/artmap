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

//! # Key Conversion Trait
//!
//! Provides the [`AsBytes`] trait used for byte-lexicographical indexing in ART.

use byteslice::ByteSlice;

/// A trait for types that can be borrowed as a byte slice for radix indexing.
pub trait AsBytes {
    /// Returns the key as a contiguous byte slice.
    fn as_bytes(&self) -> &[u8];
}

impl AsBytes for [u8] {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        self
    }
}

impl<const N: usize> AsBytes for [u8; N] {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsBytes for Vec<u8> {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsBytes for str {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsBytes for String {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsBytes for ByteSlice {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<T: AsBytes + ?Sized> AsBytes for &T {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        (*self).as_bytes()
    }
}

impl AsBytes for u8 {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        std::slice::from_ref(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_as_bytes() {
        assert_eq!(b"hello".as_bytes(), b"hello");
        assert_eq!("world".as_bytes(), b"world");
        assert_eq!(String::from("surreal").as_bytes(), b"surreal");
        assert_eq!(vec![1, 2, 3].as_bytes(), &[1, 2, 3]);
        assert_eq!(ByteSlice::from("bytes").as_bytes(), b"bytes");
    }
}
