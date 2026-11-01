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
// it is the concern's ThreadingAPI. This struct is the pool's five operations.
//
// Dispatch: through the engine's registry channel — this package's served
// ThreadPoolAPI, selected by the package's generated dispatch id
// (PACKAGE_THREAD_POOL, the generated package_types.h). The static rendering is
// the plain-externs dialect (zero indirection).
//
// The unit-of-work contract (what runs in a job): a job is pure computation
// into caller-owned memory — it never proxies DOM/JS work to the main thread
// and never blocks on another job. The barrier (WaitAll) is the happens-before:
// on return, every submitted job's writes are visible to the calling thread.

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
  void (*Submit)(CruxThreadPool *pool, void (*fn)(void *arg), void *arg);

  // The batch barrier: block (native) / spin (web main thread, which cannot
  // block) until every submitted job has completed — the happens-before. In
  // serial mode this is a no-op (the jobs already ran inline).
  void (*WaitAll)(CruxThreadPool *pool);

  // The pool's worker count (0 in serial mode, or for NULL).
  int (*WorkerCount)(CruxThreadPool *pool);
} ThreadPoolAPI;

#ifdef __cplusplus
}
#endif

#endif
