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

//! Synchronisation primitives used by the latch and the node layouts.
//!
//! Under `cfg(loom)` these are loom's model-checked types, so the loom models
//! in `loom_tests` exercise the real latch and node code (§16.3). Otherwise
//! they are the `std` types.

#[cfg(loom)]
pub(crate) mod atomic {
    pub(crate) use loom::sync::atomic::{
        fence, AtomicBool, AtomicIsize, AtomicPtr, AtomicU16, AtomicU32, AtomicU64, AtomicU8,
        Ordering,
    };
}

#[cfg(not(loom))]
pub(crate) mod atomic {
    pub(crate) use std::sync::atomic::{
        fence, AtomicBool, AtomicIsize, AtomicPtr, AtomicU16, AtomicU32, AtomicU64, AtomicU8,
        Ordering,
    };
}

#[cfg(loom)]
pub(crate) use loom::hint::spin_loop;
#[cfg(not(loom))]
pub(crate) use std::hint::spin_loop;

#[cfg(loom)]
pub(crate) use loom::thread::yield_now;
#[cfg(not(loom))]
pub(crate) use std::thread::yield_now;

/// Declares a function that is `const` in normal builds and a plain `fn`
/// under loom, whose atomics have no `const fn new`.
macro_rules! const_fn_unless_loom {
    ($(#[$meta:meta])* $vis:vis fn $name:ident($($args:tt)*) -> $ret:ty $body:block) => {
        #[cfg(not(loom))]
        $(#[$meta])*
        $vis const fn $name($($args)*) -> $ret $body

        #[cfg(loom)]
        $(#[$meta])*
        $vis fn $name($($args)*) -> $ret $body
    };
}
pub(crate) use const_fn_unless_loom;
