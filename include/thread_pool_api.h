#ifndef CRUX_THREAD_POOL_API_H
#define CRUX_THREAD_POOL_API_H

#include "kernel_types.h"
#include "thread_pool_types.h"

#ifdef __cplusplus
extern "C" {
#endif

// The thread_pool standalone's contract: the one struct of function pointers
// the package serves. This is the POOL layer — the "give me workers and a batch
// barrier" composition on the threading primitive (the concern it requires).
// The primitive itself (opaque thread handle, the hardware facts) is NOT here;
// it is the concern's ThreadingAPI. The served struct is append-only: new
// operations are added, existing ones never move.
//
// Dispatch: through the engine's registry channel — this package's served
// ThreadPoolAPI, selected by the package's generated dispatch id
// (PACKAGE_THREAD_POOL, the generated package_types.h). The static rendering is
// the plain-externs dialect (zero indirection).
//
// HOW TO USE IT RIGHT (the whole discipline, in one place):
//
//   A BATCH is the span between one WaitAll's return and the next.
//
//     CruxThreadPool *pool = Create(n);        // once, at startup
//     for (each frame) {
//       ... optionally SetMode(pool, mode);    // for the batch about to open
//       Submit(pool, fn, arg);                 // push the frame's jobs
//       ...
//       SubmitSlot(pool, slot, fn, arg);       // (or pin work to a slot)
//       WaitAll(pool);                         // close the batch; wait for all
//       ... use the results (the barrier is the happens-before) ...
//     }
//     Destroy(pool);                           // once, at teardown
//
//   1. ONE submitting thread per batch (mechanically enforced). The push path
//      is lock-free, so two threads pushing to the same batch is a data race:
//      the pool records the first push's thread per batch and REFUSES every
//      push from a different thread NAMED (the job drops; the batch counter
//      does not move, so the barrier still closes correctly). The submitter
//      may change ACROSS batches (the id re-arms at every barrier), never
//      within one.
//   2. ALL of a batch's submits complete before its WaitAll (SetMode included
//      — it writes the batch's plain state, which the workers read across the
//      barrier's lock handshake).
//   3. A JOB is pure computation into caller-owned memory: it never proxies
//      DOM/JS work to the main thread (the web barrier spins on the main
//      thread and would deadlock on such a proxy), never blocks on another
//      job, never calls back into the pool (a worker-side WaitAll is a
//      deadlock), and its `arg` storage outlives the batch. On WaitAll's
//      return, every job's writes are visible to the calling thread; ordering
//      AMONG parallel jobs is unspecified (a batch boundary is the ordering).
//   4. THE MODE is a per-batch fact: SetMode names it for the batch the NEXT
//      WaitAll opens; the barrier re-arms the default (LANES) after every
//      batch. LANES is the performant default; STEAL is for skewed batches
//      (see TPMode for when it pays).
//   5. THE BATCH BOUND: each slot tracks at most 4096 jobs per batch (a total
//      of workerCount × 4096). A slot that outgrows its window drops the
//      excess jobs NAMED (the log line names it; the barrier still closes).
//      Size a caller's arena for the per-slot bound.
//   6. NO IN-FLIGHT BATCH AT DESTROY: the last WaitAll closes before Destroy
//      (Destroy signals + joins; unrun jobs are released, not run).

typedef struct ThreadPoolAPI {
  PackageVersion version;

  // Create a pool. workerCount <= 0 (auto) sizes to the hardware's PHYSICAL
  // core count (Thread_Count_Physical — exact on native, an honest estimate on
  // web); workerCount > 0 is an explicit override. Either way the count is
  // clamped to the concurrency ceiling (Thread_Concurrency). A ceiling of 0
  // (threading_none, or an Emscripten page without SharedArrayBuffer) creates
  // the pool in SERIAL mode: no workers, jobs run inline on the submitting
  // thread, WaitAll is a no-op, WorkerCount reads 0. Returns NULL only on
  // allocation/spawn failure (a serial pool never fails to create).
  CruxThreadPool *(*Create)(int workerCount);

  // Destroy the pool: signal the workers to stop, join them, release any
  // queued-but-unrun jobs, reclaim the pool. NULL is a no-op.
  void (*Destroy)(CruxThreadPool *pool);

  // Enqueue a job (fn(arg)) for the workers. In serial mode the job runs
  // INLINE on the calling thread (there are no workers). fn must be self-
  // contained with respect to other jobs (the unit-of-work contract above).
  void (*Submit)(CruxThreadPool *pool, void (*func)(void *arg), void *arg);

  // The batch barrier: block (native) / spin (web main thread, which cannot
  // block) until every submitted job has completed — the happens-before. In
  // serial mode this is a no-op (the jobs already ran inline).
  void (*WaitAll)(CruxThreadPool *pool);

  // The pool's worker count (0 in serial mode, or for NULL).
  int (*WorkerCount)(CruxThreadPool *pool);

  // Select the batch dispatch mode (TPMode, the per-batch fact): it takes
  // effect for the batch the NEXT WaitAll opens (the barrier re-arms the
  // default, TP_MODE_LANES, after every batch). TP_MODE_STEAL is the expert
  // mode for skewed distributions. NULL is a no-op; serial pools keep the
  // value (it is inert: there are no workers to dispatch to).
  void (*SetMode)(CruxThreadPool *pool, TPMode mode);

  // Enqueue a job into the worker slot's OWN lane (lanes mode) / deque
  // (steal mode) - the caller's distribution fact. slot is 0..WorkerCount-1;
  // an out-of-range slot is a named refusal (the job drops, the batch counter
  // does not move). Submit is the round-robin form of this. The expert use:
  // pin a known-skewed batch (all heavy jobs into one slot) so the steal mode
  // can rebalance it.
  void (*SubmitSlot)(CruxThreadPool *pool, int slot, void (*func)(void *arg), void *arg);

  // Create a pool whose job-NODE memory is owned by the caller's allocator
  // (the TPAllocator seam, thread_pool_types.h): a hot arena (claim / no-op
  // free / barrier reset), a polyglot caller's own allocator over the ABI, or
  // NULL for the memory-concern fallback (identical to Create). The sizing /
  // serial-degrade / worker-spawn behavior is Create's, exactly. A non-NULL
  // allocator with a missing function is a named refusal (NULL).
  CruxThreadPool *(*CreateWithAllocator)(int workerCount, const TPAllocator *allocator);
} ThreadPoolAPI;

#ifdef __cplusplus
}
#endif

#endif
