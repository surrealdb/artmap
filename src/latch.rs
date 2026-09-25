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

//! # Optimistic Version Latch
//!
//! Provides [`HybridLatch`], an atomic version latch implementing the
//! Read-Optimized Write-EXclusion (ROWEX) / Optimistic Lock Coupling (OLC) protocol.

use std::sync::atomic::{AtomicU64, Ordering};

pub const LOCK_BIT: u64 = 0b01;
pub const OBSOLETE_BIT: u64 = 0b10;
pub const LOCKED_OR_OBSOLETE: u64 = LOCK_BIT | OBSOLETE_BIT;
pub const VERSION_STEP: u64 = 0b100;

/// Error returned when acquiring a lock on an obsolete node.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum LockError {
    Obsolete,
}

/// An atomic version latch supporting optimistic non-blocking reads and exclusive writes.
#[repr(transparent)]
#[derive(Debug)]
pub struct HybridLatch {
    version: AtomicU64,
}

impl Default for HybridLatch {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl HybridLatch {
    /// Attempts to acquire the exclusive write lock if the version matches `expected_version`.
    #[inline]
    pub fn lock_version(&self, expected_version: u64) -> Result<u64, LockError> {
        if self.version.load(Ordering::Relaxed) != expected_version {
            return Err(LockError::Obsolete);
        }
        match self.version.compare_exchange(
            expected_version,
            expected_version | LOCK_BIT,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => Ok(expected_version),
            Err(_) => Err(LockError::Obsolete),
        }
    }

    /// Creates a new unlocked, non-obsolete latch with initial version 0.
    #[inline]
    pub const fn new() -> Self {
        Self {
            version: AtomicU64::new(0),
        }
    }

    /// Reads the current version if the latch is neither write-locked nor obsolete.
    ///
    /// Returns `Some(version)` if unlocked and live, or `None` if currently locked or obsolete.
    #[inline]
    pub fn read_version(&self) -> Option<u64> {
        let v = self.version.load(Ordering::Acquire);
        if v & LOCKED_OR_OBSOLETE != 0 {
            None
        } else {
            Some(v)
        }
    }

    /// Validates that the latch has not changed, been locked, or marked obsolete
    /// since `start_version` was captured.
    #[inline]
    pub fn validate(&self, start_version: u64) -> bool {
        std::sync::atomic::fence(Ordering::Acquire);
        self.version.load(Ordering::Relaxed) == start_version
    }

    /// Checks if the node has been marked obsolete.
    #[inline]
    pub fn is_obsolete(&self) -> bool {
        self.version.load(Ordering::Acquire) & OBSOLETE_BIT != 0
    }

    /// Attempts to acquire the exclusive write lock without spinning.
    ///
    /// Returns `Ok(current_version)` on success, or `Err(true)` if obsolete, `Err(false)` if locked.
    #[inline]
    pub fn try_lock(&self) -> Result<u64, bool> {
        let v = self.version.load(Ordering::Relaxed);
        if v & OBSOLETE_BIT != 0 {
            return Err(true);
        }
        if v & LOCK_BIT != 0 {
            return Err(false);
        }
        match self.version.compare_exchange_weak(
            v,
            v | LOCK_BIT,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => Ok(v),
            Err(_) => Err(false),
        }
    }

    /// Acquires the exclusive write lock, spinning with exponential backoff until acquired.
    ///
    /// Returns `Ok(current_version)` on success, or `Err(LockError::Obsolete)` if the node becomes obsolete.
    #[inline]
    pub fn lock(&self) -> Result<u64, LockError> {
        let mut backoff = SpinBackoff::new();
        loop {
            match self.try_lock() {
                Ok(v) => return Ok(v),
                Err(true) => return Err(LockError::Obsolete),
                Err(false) => backoff.spin(),
            }
        }
    }

    /// Releases the exclusive write lock, incrementing the version counter to notify readers.
    #[inline]
    pub fn unlock(&self) {
        let current = self.version.load(Ordering::Relaxed);
        debug_assert_ne!(current & LOCK_BIT, 0, "unlock called on un-locked latch");
        let new_version = (current & !LOCK_BIT).wrapping_add(VERSION_STEP);
        self.version.store(new_version, Ordering::Release);
    }

    /// Marks the node as permanently obsolete and releases the lock.
    #[inline]
    pub fn mark_obsolete_and_unlock(&self) {
        let current = self.version.load(Ordering::Relaxed);
        debug_assert_ne!(
            current & LOCK_BIT,
            0,
            "mark_obsolete called on un-locked latch"
        );
        let new_version = (current & !LOCK_BIT) | OBSOLETE_BIT;
        self.version.store(new_version, Ordering::Release);
    }
}

/// Exponential backoff helper for spinning writers.
pub struct SpinBackoff {
    step: u32,
}

impl Default for SpinBackoff {
    fn default() -> Self {
        Self::new()
    }
}

impl SpinBackoff {
    #[inline]
    pub const fn new() -> Self {
        Self { step: 0 }
    }

    #[inline]
    pub fn spin(&mut self) {
        if self.step <= 10 {
            let spins = 1 << self.step.min(8);
            for _ in 0..spins {
                std::hint::spin_loop();
            }
        } else {
            std::thread::yield_now();
        }
        if self.step < 16 {
            self.step += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_latch_lifecycle() {
        let latch = HybridLatch::new();
        assert!(!latch.is_obsolete());

        let v0 = latch.read_version().expect("should read initial version");
        assert!(latch.validate(v0));

        let v_lock = latch.lock().expect("should acquire lock");
        assert_eq!(v_lock, v0);
        assert!(
            latch.read_version().is_none(),
            "read_version must return None while locked"
        );
        assert!(!latch.validate(v0), "validation must fail while locked");

        latch.unlock();
        let v1 = latch.read_version().expect("should read updated version");
        assert_eq!(v1, v0 + VERSION_STEP);
        assert!(!latch.validate(v0));
        assert!(latch.validate(v1));

        let _ = latch.lock().expect("should acquire lock again");
        latch.mark_obsolete_and_unlock();
        assert!(latch.is_obsolete());
        assert!(latch.read_version().is_none());
        assert!(!latch.validate(v1));
        assert_eq!(
            latch.lock(),
            Err(LockError::Obsolete),
            "cannot lock obsolete node"
        );
    }
}
