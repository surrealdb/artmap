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

//! # Deterministic interleaving hooks (§16.5)
//!
//! Compiled out unless built with `--cfg artmap_hooks`. Tests install a
//! per-thread hook that runs at named points of the reader and writer
//! protocols, which makes rare races deterministic.

/// A named point in the reader or writer protocol.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Point {
    /// A point lookup has loaded the root pointer.
    ReaderAfterRoot,
    /// A point lookup has read a node's version, before validating its parent.
    ReaderAfterChildVersion,
    /// A cursor seek has read a node's version.
    CursorSeek,
    /// A writer has read a node's version during its optimistic descent.
    WriterAfterVersion,
    /// A writer is about to upgrade or acquire latches.
    WriterBeforeUpgrade,
}

#[cfg(not(artmap_hooks))]
#[inline(always)]
pub(crate) fn pause(_point: Point) {}

#[cfg(artmap_hooks)]
mod imp {
    use super::Point;
    use std::cell::RefCell;

    type Hook = Box<dyn FnMut(Point)>;

    thread_local! {
        static HOOK: RefCell<Option<Hook>> = const { RefCell::new(None) };
    }

    /// Runs the current thread's hook, if any.
    pub fn pause(point: Point) {
        let hook = HOOK.with(|h| h.borrow_mut().take());
        if let Some(mut f) = hook {
            f(point);
            HOOK.with(|h| {
                let mut slot = h.borrow_mut();
                if slot.is_none() {
                    *slot = Some(f);
                }
            });
        }
    }

    /// Installs `f` as this thread's hook.
    pub fn set(f: impl FnMut(Point) + 'static) {
        HOOK.with(|h| *h.borrow_mut() = Some(Box::new(f)));
    }

    /// Removes this thread's hook.
    pub fn clear() {
        HOOK.with(|h| *h.borrow_mut() = None);
    }
}

#[cfg(artmap_hooks)]
pub use imp::{clear, pause, set};
