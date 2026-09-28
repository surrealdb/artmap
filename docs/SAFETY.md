# artmap safety model

This document is normative for artmap's `unsafe` code. Every `// SAFETY:`
comment cites the invariants below (`Inv N`), the latch rules (`W1`–`W5`,
`R1`–`R6`), or a section (`§N`). Section numbers follow the safety and
soundness plan this model was derived from, so references in the code and in
the plan agree.

## §3 What "sound" means

No undefined behaviour, data race or use-after-free is reachable from safe
code under any concurrency; under adversarial but safe user code (`AsBytes`,
`Clone`, `Drop`, `Debug` and closures may panic, be non-deterministic, or
re-enter the map); under subtyping and variance; with guards from any source;
under misuse of public helper types; and with hostile sizes (very long keys,
full or tiny arenas).

Liveness: a panic in user code never leaves a latch locked or a half-mutated
node published; the map stays usable. No operation recurses in proportion to
key length or tree depth. Dropping a map releases every key and value.

Consistency: point operations are linearizable; iterators yield every key
present for the whole scan exactly once, in order; `clear()` is linearizable
at its root swap; `len()` is exact at quiescence and never underflows.

## §4 Invariants

**Inv 1 — Immutable publication; retire exactly once.** Once a leaf or a
version node is reachable, its key, value, version and tombstone state are
never mutated, moved out, or dropped in place. Replacing means allocating a
new object, installing it with a `Release` store under the owning latch,
flagging the old object (`removed`/`superseded`) before unlocking, and
retiring it exactly once after it is unreachable. Inner nodes are different:
their header fields and child slots are mutated in place under the node latch,
following Inv 5.

**Inv 2 — Map-bound, guard-bound borrows.** Every reference into an EBR map
is bounded by both a borrow of the map and a live epoch guard (owned, shared
or borrowed). Arena references are bounded by the map borrow alone, because
arena memory is never reused while the map is borrowed. Tree drop is
synchronous and runs only when no borrow exists.

**Inv 3 — No `&mut` to published memory.** Published nodes and leaves are
accessed only through `&T` with atomics and `UnsafeCell`, or through raw
pointers. `&mut` is permitted only on allocations no other thread can reach
(unpublished nodes being built, or exclusively owned ones). Arena
initialisation writes go through raw places.

**Inv 4 — Strict provenance.** No integer-to-pointer casts in artmap code.
Tag bits are manipulated with `map_addr`; retirement goes through
`Retired<T>`; arena offsets are integers re-based on a provenance-carrying
base pointer.

**Inv 5 — Latch protocol.** See §5.

**Inv 6 — Unwind and re-entrancy safety.** Every latch-held region has a
*prepare* phase, which may unwind and writes nothing another thread can see,
and a *commit* phase, which contains only stores to published memory followed
by unlock or mark-obsolete, inside an `AbortOnUnwind` scope. User code
(`AsBytes`, `Clone`, `Drop`, closures) never runs while a latch is held.
Fallible calls (arena allocation, `expect`, indexing) never run in a commit
phase. Every operation pins once, before its first latch, and never pins,
flushes or repins while holding a latch. Discarded keys and values are dropped
after unlocking. Latch guards are RAII.

**Inv 7 — Structural moves, obsolescence and lock order.** An operation that
detaches a node (growth, `clear`) or changes its absolute key path (prefix
split) holds that node's latch and bumps or obsoletes its version before the
parent's latch is released. `OBSOLETE` is set only on nodes already unlinked.
The lock order is `root_latch` → parent → child → `chain_latch`. Parents and
`root_latch` are taken with a blocking `lock()` while holding nothing,
followed by a pointer re-check; nodes below a held latch are taken only by a
version-checked `try_upgrade`. `chain_latch` is terminal: while holding it,
code never takes another latch, allocates from an arena, pins, or runs user
code.

**Inv 8 — Latched writes only; no rollback.** Every write to a published
node's child slot, bitmap, key byte, count, prefix or `exact_leaf`, to `root`,
or to a version chain's `head`/`next`, happens while holding the owning latch.
A published pointer is never rolled back. There is no lock-free insert path.
Every load of `root` is `Acquire`.

**Inv 9 — Invariance and auto traits.** All maps, trees and inserters are
invariant in `K` and `V` (`PhantomData<(L, fn(L) -> L)>`). Handles, iterators,
guards and inserters are `!Send` and `!Sync`. Maps are `Sync` iff
`K, V: Send + Sync`.

**Inv 10 — User traits are untrusted.** Unsafe code never relies on `AsBytes`
being deterministic, having stable lengths, or agreeing with `Borrow`. Every
index derived from key bytes is bounds-checked. The bytes used for an install
are derived once, from the key's final location in the leaf; probes on a
caller's key are hints.

**Inv 11 — One reclamation domain.** All pins and retirements use the default
crossbeam collector, through artmap's own per-thread participant. Public APIs
accept only `artmap::Guard<'m>`, which always wraps a real pin.

**Inv 12 — Accounting inside the critical section.** `len` is incremented
before the `Release` store that makes a new live entry reachable, and
decremented after the store that unlinks one, inside the same critical
section. `len` is a signed counter, clamped at zero only as a defensive net.

**Inv 13 — Bounded stack.** No algorithm recurses in proportion to key length
or tree depth.

## §5 Latch protocol

Writers:

- **W1** Acquire with an `Acquire` CAS, then immediately `fence(Release)`.
- **W2** After the fence, scalar stores (key bytes, prefix words, counts) may
  be `Relaxed`.
- **W3** Pointer stores are always `Release`; readers load pointers with
  `Acquire`.
- **W4** `unlock` stores `v + STEP` with `Release`; `mark_obsolete` stores
  `v | OBSOLETE`, only for unlinked nodes.
- **W5** Every field an optimistic reader reads is atomic and written at the
  width it is read.

Readers:

- **R1** `v = read_version()` (`Acquire`); locked or obsolete means retry.
- **R2** Load each field at most once per optimistic section, clamp what is
  derived from it, and use `>=`/`<` checks.
- **R3** Never conclude anything (return, lock, compare as final) from
  unvalidated reads. Following a non-null child before validation is fine.
- **R4** After reading a child's version, validate the parent; at the root,
  re-check that `root` still points at the node.
- **R5** `validate(v)` is `fence(Acquire)` then a `Relaxed` load equal to `v`.
- **R6** A null child slot is absent; it is never dereferenced.

## §8.2 Guards and handles

Every artmap pin goes through an artmap-owned per-thread participant, tagged
with an ID. A handle is duplicated with a nested pin only after checking that
the current participant has the same ID; during thread-local destruction, when
that cannot be proven, duplication panics. Owned iterators share one
`Rc<Guard>` with their items; `*_with_guard` APIs borrow the caller's guard.

## §9 Writers

Every write: pin; optimistic descent with coupling, comparing existing keys
before any latch; prepare unpublished nodes; acquire the parent (blocking,
then re-check) and the node (`try_upgrade`); commit inside `AbortOnUnwind`
with `len` accounting before the publishing store; unlock; then retire
displaced objects. Retries reuse prepared nodes.

§9.3: a new leaf is owned by an `Unpublished` guard until the commit phase
disarms it, so a discarded leaf is freed after the install returns, outside
any latch, and after the last use of the key bytes derived from it.

§9.8 `clear()`: swap the root under `root_latch`, then walk the detached tree
holding one node latch at a time, marking each node obsolete, retiring nodes
and leaves individually, and subtracting the number of leaves found.

## §10 Readers and the cursor

Point lookups follow §5 with coupling. The cursor keeps `(node, version,
next byte)` frames, validates each step, and on failure re-seeks from an
owned copy of the last key it yielded. Unbounded back ends are rightmost
descents; the two ends of a double-ended scan stop when they meet.

## §11 Version chains

Every chain mutation holds the leaf's `chain_latch`. Positions are found under
the latch. Same-version replacement is out of place. Inline slots are owned by
the leaf and never retired independently. `len` changes by the liveness of
the head before and after, decided under the latch. `prune_key` evaluates the
user's `is_tombstone` without any latch, re-checks under the latch, and
retires every detached heap node exactly once.

## §12 Arena

The arena buffer is aligned to 64 bytes; allocation is a check-then-bump CAS
that honours each type's alignment and never advances on failure. Arena maps
drop every key and value exactly once when dropped: the live tree, the
version chains, and a retired list of every leaf or version node unlinked
while the map was alive. Allocation happens only in prepare phases; a failed
allocation releases every latch and returns the caller's key and value.
