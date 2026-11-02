#ifndef CRUX_THREAD_POOL_TYPES_H
#define CRUX_THREAD_POOL_TYPES_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// The opaque pool handle. The pool is a thin composition on the threading
// primitive: a set of worker threads (each a CruxThread the primitive made)
// draining a batch dispatch (the TPMode below), plus a batch barrier
// (WaitAll). The caller tracks the handle it is handed and passes it back; it
// is never dereferenced. A pool whose concurrency ceiling is 0 is created in
// SERIAL mode (no workers; jobs run inline on the submitting thread) and
// reports a worker count of 0.
typedef struct CruxThreadPool CruxThreadPool;

// The batch dispatch mode: how a batch's jobs reach the workers.
//
// TP_MODE_LANES (the default): each worker runs its own lane - the
// round-robin share of the batch - with no stealing and no shared state on
// the hot path. The performant choice for EVEN work: nothing to steal, so no
// stealing cost.
//
// TP_MODE_STEAL (the expert mode): per-worker private lock-free deques
// (Chase-Lev) plus cross-worker stealing - a worker that drains its own share
// first PULLS jobs off the busy workers' deques. It recovers the load
// balance LANES loses when the caller's distribution is skewed (a cluster of
// heavy jobs in a few slots: measured ~8x recovery where lanes stall near
// 1.1x). Its tax: a seq_cst fence per pop/steal, paid even when there is
// nothing to steal.
//
// The mode is a PER-BATCH fact: SetMode takes effect for the batch the next
// WaitAll opens (the barrier re-arms the default after every batch), so a
// caller chooses per batch without re-creating the pool. Rule of thumb: keep
// the default; switch a batch to STEAL when that batch is known to be skewed
// (SubmitSlot-pinned heavy work, or a frame with a localized hot region).
typedef enum TPMode {
  TP_MODE_LANES = 0,
  TP_MODE_STEAL = 1,
} TPMode;

// The job-NODE allocator: how the pool gets and gives back the memory a job's
// node (its fn + arg pair) lives in. This is the pool's MEMORY-OWNERSHIP
// SEAM: the pool never assumes who owns job-node memory, so
// (a) a crux caller can wire a hot arena (the org's arena package: alloc =
// Claim, free = no-op, reset = the barrier's bulk Reset — the measured
// zero-malloc hot path), (b) a polyglot caller over the ABI (Rust/Zig/C++/
// Odin — any language that works over this C ABI) manages its own allocations
// and simply supplies its own three functions, and (c) NOBODY — the general
// case, the arena is an optional concern — the pool falls back to the memory
// concern (Memory_Alloc / Memory_Free) directly.
//
// The contract (checked at create: a missing required function is a named
// refusal): alloc and reset are non-NULL; free MAY be NULL. alloc returns a
// node of `size` bytes (the pool hands it sizeof(TPJob)). When free is
// non-NULL it releases the node after its run; when it is NULL there is no
// per-node release at all - reset then MUST reclaim the batch's nodes, and a
// NULL free paired with a no-op reset LEAKS the nodes (the caller's fact,
// named here and at the create-time check). reset runs at every barrier (the
// batch boundary). free and reset are COMPLEMENTARY: free per node with a
// no-op reset (a memory-style allocator) or a NULL free with a bulk reset
// (a bump arena) - the pool calls free after every run when present and
// reset at every barrier, and does no per-node bookkeeping of its own (the
// zero-alloc payoff).
typedef struct TPAllocator {
  void *ctx; // the allocator's state (an arena, a language allocator, NULL)
  void *(*alloc)(void *ctx, size_t size);
  void (*free)(void *ctx, void *ptr);
  void (*reset)(void *ctx);
} TPAllocator;

#ifdef __cplusplus
}
#endif

#endif
