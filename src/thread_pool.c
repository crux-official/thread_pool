// The worker-thread POOL — a thin, platform-agnostic composition on the
// threading PRIMITIVE (the concern this package requires). The logic is
// shared; the platform branches are (a) the low-level lock/cond (pthread on
// posix and web, SRWLOCK + condition variables on win32) and (b) the barrier's
// wait strategy (block on native, spin on the web main thread — which cannot
// block: Atomics.wait is illegal there, and emscripten's main-thread blocking
// deadlocks when a worker proxies to it).
//
// THE BATCH DISPATCH: a batch is the span between one WaitAll's return and
// the next. Jobs enter through Submit/SubmitSlot, leave through WaitAll.
//
// (1) THE DISPATCH MODE (TPMode, a per-batch caller fact): LANES (the
// performant default) runs each worker's round-robin lane with no stealing
// and no shared hot state; STEAL (the expert mode for skewed distributions)
// runs per-worker private lock-free deques (the correct Chase-Lev form: the
// owner's pop claims the slot first, one seq_cst fence per op, the one-item
// race is a seq_cst CAS on the shared top) plus cross-worker stealing — it
// recovers the load balance lanes loses on a lopsided batch (measured ~8x
// where lanes stall near 1.1x). SubmitSlot is the distribution seam (the
// caller pins work to a slot; Submit is the round-robin form).
//
// (2) THE ALLOCATOR SEAM (TPAllocator): the pool never assumes who owns
// job-node memory. It calls alloc/free/reset through the allocator it was
// handed (CreateWithAllocator) — a hot arena (Claim / NULL free / barrier
// Reset), a polyglot caller's own allocator over the ABI, or the memory
// concern directly when nobody hands it one (Create — the general case; the
// arena is an optional concern). The per-job hot path is ONE alloc call
// (a bump claim on the arena path: no malloc) and ONE done-count atomic add
// (only the LAST completer takes the lock to wake the barrier).
//
// (3) THE BARRIER AS BATCH BOUNDARY: WaitAll opens the batch (a seq edge
// wakes the workers — a worker drains exactly once per batch, so a fast
// worker cannot re-enter a batch it already drained), then runs TWO phases
// before the boundary: (a) wait for the happens-before (every job done),
// (b) wait for every worker to be PARKED (out of the drain). The second
// phase is load-bearing: a trailing worker's pop claim can straddle the
// re-arm's deque reset, and a claim straddling it reads a stale items[]
// slot — a freed node of the batch just released — and runs it (a
// use-after-free; its rarer face is a lost job that parks the barrier
// forever). Then WaitAll releases the batch's nodes through the allocator's
// reset and re-arms the dispatch state — all under the lock (atomic w.r.t.
// the workers; a stale done count can never leak into the next batch).
//
// THE SUBMISSION DISCIPLINE (mechanically checked): ONE submitting thread
// per batch, and the batch's submits complete before its WaitAll. The push
// path is lock-free (plain lane lengths, the owner's deque bottom), so a
// second concurrent submitter is a data race on that state — SubmitSlot
// records the first push's thread id per batch and refuses every push from
// a different thread NAMED (the job drops, the batch counter does not move).
// A second submitter would also be a second owner of a deque's bottom in
// steal mode — the Chase-Lev model has exactly one owner per deque.
//
// The workers themselves are the primitive's threads: each is a CruxThread the
// concern's Thread_Create made (pthread on native, an emscripten Web Worker on
// web) running this package's worker loop (drain the batch until shutdown).
// The package never touches the platform thread API directly — it speaks the
// primitive, which is why one source serves every platform.
//
// Sizing (Create / CreateWithAllocator): auto (n <= 0) sizes to the
// hardware's PHYSICAL core count (Thread_Count_Physical — exact on native, an
// honest estimate on web); an explicit n overrides. Either is clamped to the
// concurrency ceiling (Thread_Concurrency). A ceiling of 0 — threading_none,
// or an Emscripten page without SharedArrayBuffer — creates the pool in
// SERIAL mode: no workers, Submit runs the job inline on the submitting
// thread (the allocator is not even consulted — the serial path keeps memory
// out entirely), WaitAll is a no-op, WorkerCount reads 0, and a named log
// line says so. The degradation is keyed off the primitive's concurrency
// fact, so the package stays platform-agnostic.
//
// The batch bound: each slot's lane (lanes mode) and deque (steal mode)
// track at most TP_BATCH_BOUND jobs per batch — a total batch of
// workerCount × TP_BATCH_BOUND. A slot that outgrows its window drops the
// excess jobs NAMED (the log line names it; the batch counter does not move
// — a dropped job must not open the barrier it cannot close). A caller's
// arena, if any, must be sized for the per-slot bound.
//
// The job contract (thread_pool_api.h): a job is pure computation into
// caller-owned memory — no display/input/module/file/logging calls, no engine
// dispatch, and never a proxy of work back to the submitting (main, on web)
// thread. That last clause is what makes the web barrier's spin
// deadlock-free: nothing needs the main thread to respond while it waits.

#include "crux_threading.h" // the PRIMITIVE: Thread_Create/Join + the hardware facts
#include "extension.h"
#include "kernel_types.h"
#include "logging.h"
#include "memory.h"
#include "thread_pool_api.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdatomic.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <pthread.h>
#endif

// --- The platform lock/cond (the only per-OS sync). posix and web both speak
// pthreads (emscripten pthreads are real threads with the pthread API); win32
// speaks SRWLOCK + condition variables (SleepConditionVariableSRW is the wait
// that pairs with an SRWLOCK — WaitOnConditionVariable pairs with a
// CRITICAL_SECTION, and the wrong pairing is an undefined-behavior hazard on
// Windows).
#ifdef _WIN32
typedef SRWLOCK TPLock;
typedef CONDITION_VARIABLE TPCond;
static void tp_lock(TPLock *lock) {
  AcquireSRWLockExclusive(lock);
}
static void tp_unlock(TPLock *lock) {
  ReleaseSRWLockExclusive(lock);
}
static void tp_wait(TPCond *cond, TPLock *lock) {
  SleepConditionVariableSRW(cond, lock, INFINITE, 0);
}
static void tp_broadcast(TPCond *cond) {
  WakeAllConditionVariable(cond);
}
static void tp_lock_init(TPLock *lock, TPCond *condA, TPCond *condB, TPCond *condC) {
  InitializeSRWLock(lock);
  InitializeConditionVariable(condA);
  InitializeConditionVariable(condB);
  InitializeConditionVariable(condC);
}
static void tp_lock_destroy(TPLock *lock, TPCond *condA, TPCond *condB, TPCond *condC) {
  (void)lock;
  (void)condA;
  (void)condB;
  (void)condC;
}
#else
typedef pthread_mutex_t TPLock;
typedef pthread_cond_t TPCond;
static void tp_lock(TPLock *lock) {
  pthread_mutex_lock(lock);
}
static void tp_unlock(TPLock *lock) {
  pthread_mutex_unlock(lock);
}
static void tp_wait(TPCond *cond, TPLock *lock) {
  pthread_cond_wait(cond, lock);
}
static void tp_broadcast(TPCond *cond) {
  pthread_cond_broadcast(cond);
}
static void tp_lock_init(TPLock *lock, TPCond *condA, TPCond *condB, TPCond *condC) {
  pthread_mutex_init(lock, NULL);
  pthread_cond_init(condA, NULL);
  pthread_cond_init(condB, NULL);
  pthread_cond_init(condC, NULL);
}
static void tp_lock_destroy(TPLock *lock, TPCond *condA, TPCond *condB, TPCond *condC) {
  pthread_mutex_destroy(lock);
  pthread_cond_destroy(condA);
  pthread_cond_destroy(condB);
  pthread_cond_destroy(condC);
}
#endif

// The job node: what a worker runs. Packed (a fn + an arg); the node's memory
// comes from the pool's allocator (the TPAllocator seam) and goes back
// through it — the pool never owns node memory itself.
typedef struct TPJob {
  void (*fn)(void *arg);
  void *arg;
} TPJob;

// The batch bound: the pool's lanes + deques track at most this many jobs per
// batch, and a single-slot batch must fit one whole deque.
#define TP_BATCH_BOUND 4096
#define TP_DEQ_CAP 4096
#define TP_DEQ_MASK (TP_DEQ_CAP - 1)

// The worker's private deque (steal mode). items hold the JOB NODES' POINTERS
// (the allocator owns the nodes; the deque is a private window over its own
// share). Signed indices: the Chase-Lev form claims by decrementing, so
// bottom runs below zero as items are popped.
typedef struct TPDeq {
  atomic_int_least64_t top; // the shared cold end (thieves steal here)
  atomic_int_least64_t bottom; // the owner's hot end (the owner pushes/pops)
  TPJob *items[TP_DEQ_CAP];
} TPDeq;

// The per-worker thread arg (the primitive's Thread_Create takes a void*).
typedef struct TPWorker {
  struct CruxThreadPool *pool;
  int index;
  uint64_t parkedEdge; // the barrier's second phase (under the pool lock only):
                       // the seq edge this worker is PARKED at (0 = not parked
                       // at a countable edge — a wake clears it before the
                       // worker can re-enter a drain, so a stale park from an
                       // older edge can never satisfy a newer edge's gate)
} TPWorker;

struct CruxThreadPool {
  bool serial; // the named degradation (ceiling 0): inline execution, no workers
  CruxThread **workers; // the primitive's thread handles (the worker set)
  int workerCount;
  bool hasAlloc; // an allocator was handed in (the seam is live)
  TPAllocator alloc; // the job-node allocator (valid when hasAlloc)
  TPMode mode; // the per-batch fact (SetMode; re-armed by the barrier's reset)
  TPWorker *slots; // the worker threads' per-worker args
  TPDeq *deqs; // steal mode: the per-worker private deques
  TPJob **laneJobs; // lanes mode: the per-worker lanes (job node pointers)
  int *laneLen; // lanes mode: the per-worker lane lengths
  atomic_uint_fast64_t done; // jobs finished (lock-free; the barrier's fact)
  uint64_t submitted; // jobs handed to the dispatch this batch (app-thread fact)
  uint64_t seq; // batches opened (a worker drains once per seq edge)
  atomic_uintptr_t submitter; // the single-submitter check: the batch's first
                              // push's thread id (0 = no push yet this batch —
                              // re-armed at every barrier)
  TPLock lock; // the barrier's lock (the batch open + the reset)
  TPCond workCond; // a worker wakes for a batch (or the shutdown)
  TPCond doneCond; // the native barrier wakes when done meets submitted
                   // (initialized + destroyed on web too, where the barrier
                   // spins on the atomic and this is simply unused)
  TPCond parkedCond; // the barrier's second phase: the native barrier wakes
                     // when every worker has parked (the re-arm's gate)
  bool destroyed; // the shutdown flag the worker loop exits on
};

// The allocator seam, checked once at create: a non-NULL allocator must carry
// alloc and reset (free may be NULL: a bump-arena shape reclaims the nodes at
// reset, so there is no per-node release). A missing required function is a
// NAMED refusal — the pool never guesses at a half-wired seam.
static bool tp_allocator_valid(const TPAllocator *allocator) {
  return allocator->alloc != NULL && allocator->reset != NULL;
}

// The job-node claim / release: through the caller's allocator when one was
// handed in (the arena / polyglot paths), and DIRECTLY through the memory
// concern otherwise (the general case — the fallback the seam exists to make
// optional). The branch is pool-invariant (predicted; the hot path stays a
// single call either way).
static TPJob *tp_job_claim(struct CruxThreadPool *pool) {
  if (pool->hasAlloc) {
    return (TPJob *)pool->alloc.alloc(pool->alloc.ctx, sizeof(TPJob));
  }
  return (TPJob *)Memory_Alloc(sizeof(TPJob));
}
static void tp_job_free(struct CruxThreadPool *pool, TPJob *job) {
  if (pool->hasAlloc) {
    if (pool->alloc.free != NULL) {
      pool->alloc.free(pool->alloc.ctx, job);
    }
    // a NULL free: no per-node release — the allocator's reset (the barrier)
    // reclaims the nodes (the bump-arena shape)
  } else {
    Memory_Free(job); // the memory fallback frees per node; its reset is a no-op
  }
}
// The batch boundary's release: the allocator's reset (an arena's bulk
// release of the batch's nodes). The memory fallback frees per node — nothing
// to release in bulk.
static void tp_batch_release(struct CruxThreadPool *pool) {
  if (pool->hasAlloc) {
    pool->alloc.reset(pool->alloc.ctx);
  }
}

// The submitting thread's identity (the single-submitter check below). A raw
// thread-handle value that is nonzero on the supported platforms (posix: the
// thread's TCB address; win32: the pseudo-thread id).
#ifdef _WIN32
static uintptr_t tp_thread_id(void) {
  return (uintptr_t)GetCurrentThreadId();
}
#else
static uintptr_t tp_thread_id(void) {
  return (uintptr_t)pthread_self();
}
#endif

// --- The barrier's done count: every completer does a lock-free atomic add;
// only the LAST completer (count == submitted) takes the lock to wake the
// barrier. The hot path is one atomic add — no per-job lock. ---
static void tp_done(struct CruxThreadPool *pool) {
  const uint64_t count = atomic_fetch_add_explicit(&pool->done, 1, memory_order_release) + 1;
#ifndef __EMSCRIPTEN__
  if (count == pool->submitted) {
    tp_lock(&pool->lock);
    tp_broadcast(&pool->doneCond);
    tp_unlock(&pool->lock);
  }
#else
  // Web: the barrier spins on the done atomic (the main thread cannot block),
  // so nothing waits on doneCond — the release-add / acquire-load pair already
  // carries the happens-before, and the lock + no-op broadcast would only
  // contend with the main thread's open / re-arm sections.
  (void)count;
#endif
}

// --- The Chase-Lev deque (steal mode). The orderings are load-bearing — the
// owner's pop CLAIMS THE SLOT FIRST (bottom--), then a seq_cst fence, then
// reads top; the one-item case (top == bottom) is settled by a seq_cst CAS on
// the shared top — owner and thief both CAS, only one wins, the loser
// restores bottom. The steal reads top (acquire), fences, reads bottom
// (acquire), then CASes top. ONE seq_cst fence per op, not seq_cst on every
// access — the all-seq_cst form is correct but a measured pathology (the
// workers ping-pong the deque's cache lines).
static void tp_deq_push(TPDeq *deq, TPJob *job) {
  const int64_t bottom = atomic_load_explicit(&deq->bottom, memory_order_relaxed);
  deq->items[bottom & TP_DEQ_MASK] = job;
  atomic_thread_fence(memory_order_release);
  atomic_store_explicit(&deq->bottom, bottom + 1, memory_order_relaxed);
}
static int tp_deq_pop(TPDeq *deq, TPJob **outJob) {
  const int64_t bottom = atomic_load_explicit(&deq->bottom, memory_order_relaxed) - 1;
  atomic_store_explicit(&deq->bottom, bottom, memory_order_relaxed);
  atomic_thread_fence(memory_order_seq_cst);
  int64_t top = atomic_load_explicit(&deq->top, memory_order_relaxed);
  if (top <= bottom) {
    if (top == bottom) {
      if (!atomic_compare_exchange_strong_explicit(&deq->top, &top, top + 1, memory_order_seq_cst,
                                                   memory_order_relaxed)) {
        atomic_store_explicit(&deq->bottom, bottom + 1, memory_order_relaxed);
        return 0; // a thief took the last item
      }
      atomic_store_explicit(&deq->bottom, bottom + 1, memory_order_relaxed);
    }
    *outJob = deq->items[bottom & TP_DEQ_MASK];
    return 1;
  }
  atomic_store_explicit(&deq->bottom, bottom + 1, memory_order_relaxed);
  return 0;
}
static int tp_deq_steal(TPDeq *deq, TPJob **outJob) {
  int64_t top = atomic_load_explicit(&deq->top, memory_order_acquire);
  atomic_thread_fence(memory_order_seq_cst);
  const int64_t bottom = atomic_load_explicit(&deq->bottom, memory_order_acquire);
  if (top < bottom) {
    const TPJob *job = deq->items[top & TP_DEQ_MASK];
    if (!atomic_compare_exchange_strong_explicit(&deq->top, &top, top + 1, memory_order_seq_cst,
                                                 memory_order_relaxed)) {
      return 0; // lost the steal race
    }
    *outJob = (TPJob *)job;
    return 1;
  }
  return 0;
}

// The batch boundary (the dispatch half): re-arm the lanes + deques +
// counters so the next batch starts clean. Called from WaitAll under the
// lock, AFTER the allocator's reset has released the batch's nodes. The mode
// re-arms to the default (LANES) too: it is a PER-BATCH fact (SetMode names
// it for the batch the next WaitAll opens), so one STEAL batch must not
// silently leave the pool in STEAL for every batch after it (the default
// stays the default until a caller says otherwise). The submitter id
// re-arms too: the one-thread discipline is PER BATCH — the submitter may
// legally change across the boundary, not within a batch.
static void tp_rearm(struct CruxThreadPool *pool) {
  pool->submitted = 0;
  atomic_store_explicit(&pool->done, 0, memory_order_relaxed);
  atomic_store_explicit(&pool->submitter, 0, memory_order_relaxed);
  pool->mode = TP_MODE_LANES;
  for (int worker = 0; worker < pool->workerCount; worker++) {
    pool->laneLen[worker] = 0;
    atomic_store_explicit(&pool->deqs[worker].top, 0, memory_order_relaxed);
    atomic_store_explicit(&pool->deqs[worker].bottom, 0, memory_order_relaxed);
  }
}

// The worker loop: park until a NEW batch (a seq edge) or the shutdown, drain
// the worker's share, repeat. Lanes: the worker's lane (its list of job
// nodes — the caller's distribution). Steal: pop the worker's own deque,
// then steal from the others. Re-checking after a drain cannot re-enter the
// same batch: seq only moves at WaitAll.
static void tp_worker_main(void *arg) {
  TPWorker *slot = (TPWorker *)arg;
  struct CruxThreadPool *pool = slot->pool;
  const int index = slot->index;
  uint64_t mySeq = 0; // the batch this worker last drained
  for (;;) {
    tp_lock(&pool->lock);
    while (pool->seq == mySeq && !pool->destroyed) {
      // The barrier's SECOND phase, tagged per edge: parkedEdge names the
      // edge this worker is parked at, and WaitAll counts only workers tagged
      // with the edge it just opened. The tag is cleared on wake, under the
      // lock, BEFORE the worker can re-enter a drain — so a stale park from
      // an older edge (a worker woken but not yet scheduled) can never
      // satisfy a newer edge's gate, and the re-arm can never run while a
      // worker is inside a drain (a trailing pop claim straddling the reset
      // would read a stale items[] slot — a freed node of the batch just
      // released, with the allocator seam — and run it). A spurious wake
      // re-tags and re-waits (the broadcast is cheap: it only wakes the
      // barrier, which re-checks and re-sleeps).
      slot->parkedEdge = pool->seq;
      tp_broadcast(&pool->parkedCond);
      tp_wait(&pool->workCond, &pool->lock);
      slot->parkedEdge = 0; // woken (a new edge, or the shutdown): untagged
    }
    const bool shuttingDown = pool->destroyed;
    mySeq = pool->seq;
    tp_unlock(&pool->lock);
    if (shuttingDown) {
      return;
    }
    if (pool->mode == TP_MODE_LANES) {
      // No stealing, no shared state: run the lane.
      for (int i = 0; i < pool->laneLen[index]; i++) {
        const TPJob *job =
            pool->laneJobs[((size_t)index * (size_t)TP_BATCH_BOUND) + (size_t)i];
        job->fn(job->arg);
        tp_job_free(pool, (TPJob *)job);
        tp_done(pool);
      }
    } else {
      for (;;) {
        TPJob *job = NULL;
        if (tp_deq_pop(&pool->deqs[index], &job)) {
          job->fn(job->arg);
          tp_job_free(pool, job);
          tp_done(pool);
          continue;
        }
        bool stole = false;
        for (int off = 1; off < pool->workerCount; off++) {
          const int victim = (index + off) % pool->workerCount;
          if (tp_deq_steal(&pool->deqs[victim], &job)) {
            job->fn(job->arg);
            tp_job_free(pool, job);
            tp_done(pool);
            stole = true;
            break;
          }
        }
        if (!stole) {
          break; // every deque is empty: the batch is done for this worker
        }
      }
    }
  }
}

// The shared create path: sizing (the auto/ceiling rule), the record, the
// tables, the workers. allocator is NULL for the memory-concern fallback.
static CruxThreadPool *tp_create_impl(int workerCount, const TPAllocator *allocator) {
  // Auto (<=0) uses the physical count; the clamp uses the concurrency ceiling
  // (equal on native; on web the ceiling is the pre-run pool, so the clamp — not
  // the physical estimate — is what makes a no-SAB page serial).
  const int physical = Thread_Count_Physical();
  const int ceiling = Thread_Concurrency();
  int count = (workerCount > 0) ? workerCount : physical;
  if (count > ceiling) {
    count = ceiling;
  }
  if (count < 0) {
    count = 0;
  }

  struct CruxThreadPool *pool = (struct CruxThreadPool *)Memory_Calloc(1, sizeof *pool);
  if (pool == NULL) {
    LogError("[thread_pool] Create: the pool record failed to allocate");
    return NULL;
  }
  tp_lock_init(&pool->lock, &pool->workCond, &pool->doneCond, &pool->parkedCond);
  pool->hasAlloc = (allocator != NULL);
  if (allocator != NULL) {
    pool->alloc = *allocator;
  }
  pool->serial = (count == 0);
  pool->workerCount = count;
  pool->mode = TP_MODE_LANES; // the performant default (SetMode changes it)

  if (pool->serial) {
    // The named degradation (the header's ceiling-0 case): inline execution, no
    // workers, NO job memory at all (the serial path keeps memory out — the
    // allocator is never even consulted). One build serves both pages.
    LogWarning("[thread_pool] Create: the concurrency ceiling is 0 - the pool runs SERIAL (jobs run inline; no workers)");
    return pool;
  }

  pool->slots = (TPWorker *)Memory_Calloc((size_t)count, sizeof(TPWorker));
  pool->deqs = (TPDeq *)Memory_Calloc((size_t)count, sizeof(TPDeq));
  pool->laneJobs = (TPJob **)Memory_Calloc((size_t)count * (size_t)TP_BATCH_BOUND, sizeof(TPJob *));
  pool->laneLen = (int *)Memory_Calloc((size_t)count, sizeof(int));
  pool->workers = (CruxThread **)Memory_Calloc((size_t)count, sizeof(CruxThread *));
  if (pool->slots == NULL || pool->deqs == NULL || pool->laneJobs == NULL || pool->laneLen == NULL ||
      pool->workers == NULL) {
    // No worker has been spawned yet, so Destroy's join loop cannot run —
    // free the tables that exist (Memory_Free(NULL) is a named no-op in the
    // memory concern) and reclaim the record, exactly as Destroy would.
    LogError("[thread_pool] Create: a batch-dispatch table failed to allocate");
    tp_lock_destroy(&pool->lock, &pool->workCond, &pool->doneCond, &pool->parkedCond);
    Memory_Free((void *)pool->workers);
    Memory_Free((void *)pool->laneLen);
    Memory_Free((void *)pool->laneJobs);
    Memory_Free((void *)pool->deqs);
    Memory_Free((void *)pool->slots);
    Memory_Free(pool);
    return NULL;
  }
  atomic_init(&pool->submitter, 0);
  for (int i = 0; i < count; i++) {
    pool->slots[i].pool = pool;
    pool->slots[i].index = i;
    atomic_init(&pool->deqs[i].top, 0);
    atomic_init(&pool->deqs[i].bottom, 0);
  }
  for (int i = 0; i < count; i++) {
    // A worker is a primitive thread running this package's worker loop, on
    // its OWN slot (the per-worker lane/deque lives in the slot's index).
    pool->workers[i] = Thread_Create(tp_worker_main, &pool->slots[i]);
    if (pool->workers[i] == NULL) {
      // A refused worker is a pool failure: the cell declared threads and
      // cannot honor them — tear down what exists and report NULL (the call
      // site refuses to boot named; there is no silent serial fallback).
      // Signal under the lock, exactly as Destroy does: a worker parked
      // between its batch check and its wait would otherwise miss an unlocked
      // broadcast and sleep forever, deadlocking the Join below.
      tp_lock(&pool->lock);
      pool->destroyed = true;
      tp_broadcast(&pool->workCond);
      tp_unlock(&pool->lock);
      for (int j = 0; j < i; j++) {
        Thread_Join(pool->workers[j]);
      }
      // The table-failure path's teardown, exactly (the lock objects + the
      // record included — a refused worker is a pool failure either way).
      tp_lock_destroy(&pool->lock, &pool->workCond, &pool->doneCond, &pool->parkedCond);
      Memory_Free((void *)pool->workers);
      Memory_Free((void *)pool->laneLen);
      Memory_Free((void *)pool->laneJobs);
      Memory_Free((void *)pool->deqs);
      Memory_Free((void *)pool->slots);
      Memory_Free(pool);
      LogError("[thread_pool] Create: the driver refused worker %d of %d", i + 1, count);
      return NULL;
    }
  }
  return pool;
}

CruxThreadPool *ThreadPool_Create(int workerCount) {
  // The general case: no allocator handed in — the memory concern directly.
  return tp_create_impl(workerCount, NULL);
}

CruxThreadPool *ThreadPool_CreateWithAllocator(int workerCount, const TPAllocator *allocator) {
  if (allocator != NULL && !tp_allocator_valid(allocator)) {
    LogError("[thread_pool] CreateWithAllocator: the allocator is missing a required "
             "function (alloc and reset are required; free may be NULL) - the pool refuses");
    return NULL;
  }
  return tp_create_impl(workerCount, allocator);
}

void ThreadPool_Destroy(CruxThreadPool *pool) {
  if (pool == NULL) {
    return;
  }
  tp_lock(&pool->lock);
  pool->destroyed = true;
  tp_broadcast(&pool->workCond);
  tp_unlock(&pool->lock);
  for (int i = 0; i < pool->workerCount; i++) {
    Thread_Join(pool->workers[i]);
  }
  // The contract says no in-flight batch at Destroy. The allocator's reset
  // releases the batch state (an arena's bulk reset reclaims any node a
  // contract violation left unrun; the memory fallback's is a no-op — its
  // per-node frees already did the work, and a violation there leaks the
  // unrun nodes: the caller's bug, named in the contract).
  tp_batch_release(pool);
  tp_lock_destroy(&pool->lock, &pool->workCond, &pool->doneCond, &pool->parkedCond);
  if (pool->workers != NULL) {
    Memory_Free((void *)pool->workers);
  }
  if (pool->laneLen != NULL) {
    Memory_Free((void *)pool->laneLen);
  }
  if (pool->laneJobs != NULL) {
    Memory_Free((void *)pool->laneJobs);
  }
  if (pool->deqs != NULL) {
    Memory_Free((void *)pool->deqs);
  }
  if (pool->slots != NULL) {
    Memory_Free((void *)pool->slots);
  }
  Memory_Free(pool);
}

void ThreadPool_SetMode(CruxThreadPool *pool, TPMode mode) {
  if (pool == NULL) {
    return;
  }
  // The per-batch fact: the next WaitAll's open is what the workers read.
  pool->mode = mode;
}

void ThreadPool_SubmitSlot(CruxThreadPool *pool, int slot, void (*func)(void *arg), void *arg) {
  if (pool == NULL || func == NULL) {
    return;
  }
  if (pool->serial) {
    func(arg); // inline (no workers); the barrier is a no-op, the mode is inert
    return;
  }
  // The single-submitter check (the lock-free push path's invariant, made
  // mechanical): the first push of the batch records its thread, and every
  // later push must come from that same thread. A push from a different
  // thread is a data race on the batch's plain state (the lane lengths, the
  // owner's deque bottom) — so it is REFUSED named instead of raced: the job
  // drops, the batch counter does not move (a dropped job must not open the
  // barrier it cannot close). The id is a relaxed atomic: one load per push
  // (a store only on the first push of the batch, via a failed CAS).
  {
    const uintptr_t id = tp_thread_id();
    uintptr_t first = atomic_load_explicit(&pool->submitter, memory_order_relaxed);
    if (first == 0) {
      // The first push of this batch: record this thread. On a lost race the
      // CAS hands `first` the value it found (the other thread's id).
      uintptr_t expected = 0;
      atomic_compare_exchange_strong_explicit(&pool->submitter, &expected, id,
                                              memory_order_relaxed, memory_order_relaxed);
      first = (expected == 0) ? id : expected;
    }
    if (first != id) {
      LogError("[thread_pool] SubmitSlot: a second thread is submitting to the batch (one "
               "submitting thread per batch - the job drops)");
      return;
    }
  }
  if (slot < 0 || slot >= pool->workerCount) {
    // A named refusal: an out-of-range slot cannot be dispatched. The batch
    // counter does not move (a dropped job must not open the barrier it
    // cannot close).
    LogError("[thread_pool] SubmitSlot: the slot %d is out of range (the job drops)", slot);
    return;
  }
  TPJob *job = tp_job_claim(pool);
  if (job == NULL) {
    // The claim's exhaustion (an arena too small for the batch, or the memory
    // concern's) — named, and the counter does not move.
    LogError("[thread_pool] SubmitSlot: the job node failed to claim (the job drops)");
    return;
  }
  job->fn = func;
  job->arg = arg;
  if (pool->mode == TP_MODE_STEAL) {
    // The owner's deque: the push is the app thread's only write to its
    // bottom (the owner model the Chase-Lev form assumes). A full deque (a
    // single-slot batch at the bound) drops NAMED — the wrap would corrupt
    // the item the top still owns.
    TPDeq *deq = &pool->deqs[slot];
    const int64_t bottom = atomic_load_explicit(&deq->bottom, memory_order_relaxed);
    const int64_t top = atomic_load_explicit(&deq->top, memory_order_relaxed);
    if (bottom - top >= TP_DEQ_CAP) {
      LogError("[thread_pool] SubmitSlot: the batch outgrew the steal deque (the job drops)");
      tp_job_free(pool, job);
      return;
    }
    tp_deq_push(deq, job);
  } else {
    // Lanes: the node goes into the slot's lane. The caller's slot choice IS
    // the distribution — round-robin is the default (ThreadPool_Submit), a
    // skewed batch (all heavy jobs into one slot) is the caller's call, and
    // the lanes mode cannot rebalance it (that is the steal mode's job). A
    // lane is a fixed TP_BATCH_BOUND window — a slot that outgrows it drops
    // NAMED (the steal branch's same guard: the write would land in the
    // neighbor's lane, or off the end of the allocation at the last slot).
    if (pool->laneLen[slot] >= TP_BATCH_BOUND) {
      LogError("[thread_pool] SubmitSlot: the batch outgrew the lane (the job drops)");
      tp_job_free(pool, job);
      return;
    }
    pool->laneJobs[((size_t)slot * (size_t)TP_BATCH_BOUND) + (size_t)pool->laneLen[slot]] = job;
    pool->laneLen[slot]++;
  }
  pool->submitted++; // the barrier's target (every queued job counts)
}

void ThreadPool_Submit(CruxThreadPool *pool, void (*func)(void *arg), void *arg) {
  if (pool == NULL || func == NULL) {
    return;
  }
  if (pool->serial) {
    func(arg); // inline (no workers); the allocator is not consulted
    return;
  }
  // Round-robin across the workers' slots (the pool picks the slot). The
  // remainder is < workerCount, so the int cast cannot lose information.
  ThreadPool_SubmitSlot(pool, (int)(pool->submitted % (uint64_t)pool->workerCount), func, arg);
}

void ThreadPool_WaitAll(CruxThreadPool *pool) {
  if (pool == NULL) {
    return;
  }
  if (pool->serial) {
    return; // the jobs ran inline on Submit; there is nothing to wait for
  }
  if (pool->submitted == 0) {
    return; // an empty batch (nothing was pushed): NO seq edge, NO wake —
            // bumping seq here would wake the workers to drain the empty
            // batch, and their drain would race the next batch's lane fill
            // (the safety-net barrier runs every frame, most of them empty)
  }
#ifdef __EMSCRIPTEN__
  // The web main thread CANNOT block (Atomics.wait is illegal there, and
  // emscripten's main-thread blocking deadlocks when a worker proxies to it).
  // So the barrier OPENS the batch (under the lock), unlocks, and SPINS on
  // the atomic completion counter (Atomics.load is legal on the main thread).
  // It is deadlock-free BECAUSE of the job contract: a job is pure computation
  // and never proxies work to the main thread, so nothing needs it to respond
  // while it spins. The cost is a briefly unresponsive tab for the batch's
  // duration — right for well-sized jobs (a job that needs the main thread is
  // a contract violation, not a long batch).
  tp_lock(&pool->lock);
  pool->seq++;
  tp_broadcast(&pool->workCond);
  const uint64_t target = pool->submitted;
  tp_unlock(&pool->lock);
  while (atomic_load_explicit(&pool->done, memory_order_acquire) < target) {
    // spin (the main thread cannot block)
  }
  // The second phase (the re-arm's gate): every worker tagged parked at THIS
  // edge (computed, not counted — a stale tag from an older edge never
  // satisfies it). The main thread cannot cond_wait, so the phase spins
  // under the lock; the spin is bounded (the workers park on their next loop
  // check after the drain).
  for (;;) {
    tp_lock(&pool->lock);
    uint32_t parked = 0;
    for (int worker = 0; worker < pool->workerCount; worker++) {
      if (pool->slots[worker].parkedEdge == pool->seq) {
        parked++;
      }
    }
    tp_unlock(&pool->lock);
    if (parked >= (uint32_t)pool->workerCount) {
      break;
    }
  }
  tp_lock(&pool->lock);
  tp_batch_release(pool);
  tp_rearm(pool);
  tp_unlock(&pool->lock);
#else
  tp_lock(&pool->lock);
  // Open the batch (the seq edge wakes the workers), wait for the happens-
  // before, then for the second phase (every worker parked — the re-arm's
  // gate: a trailing worker's pop claim straddling the reset would read a
  // stale items[] slot, a freed node of the batch just released, and run
  // it), release the batch's nodes through the allocator, re-arm — all under
  // the lock, so the boundary is atomic w.r.t. the workers and a stale done
  // count can never leak into the next batch.
  pool->seq++;
  tp_broadcast(&pool->workCond);
  while (atomic_load_explicit(&pool->done, memory_order_acquire) < pool->submitted) {
    tp_wait(&pool->doneCond, &pool->lock);
  }
  // The second phase (the re-arm's gate): every worker tagged parked at THIS
  // edge — computed, not counted (a stale tag from an older edge never
  // satisfies it, and a wake clears the tag before the drain).
  for (;;) {
    uint32_t parked = 0;
    for (int worker = 0; worker < pool->workerCount; worker++) {
      if (pool->slots[worker].parkedEdge == pool->seq) {
        parked++;
      }
    }
    if (parked >= (uint32_t)pool->workerCount) {
      break;
    }
    tp_wait(&pool->parkedCond, &pool->lock);
  }
  tp_batch_release(pool);
  tp_rearm(pool);
  tp_unlock(&pool->lock);
#endif
}

int ThreadPool_WorkerCount(CruxThreadPool *pool) {
  return (pool != NULL) ? pool->workerCount : 0;
}

// The served struct: the thread_pool contract, filled with this package's
// function addresses (valid from load, before Construct). The struct is
// append-only — new operations are added, existing ones never move, so an
// older consumer of the struct is untouched by a newer package.
static ThreadPoolAPI g_thread_pool_api = {
    .version = {2, 0, 0},
    .Create = ThreadPool_Create,
    .Destroy = ThreadPool_Destroy,
    .Submit = ThreadPool_Submit,
    .WaitAll = ThreadPool_WaitAll,
    .WorkerCount = ThreadPool_WorkerCount,
    .SetMode = ThreadPool_SetMode,
    .SubmitSlot = ThreadPool_SubmitSlot,
    .CreateWithAllocator = ThreadPool_CreateWithAllocator,
};

void *ThreadPool_GetSpecificAPI(void) {
  return &g_thread_pool_api;
}

// The extension interface. A standalone registers NO concern bit (it is not a
// capability — the threading concern is; this is a composition on it). Its
// GetSpecificAPI serves the ThreadPoolAPI the registry dispatches through
// PACKAGE_THREAD_POOL (the generated package_types.h).
static bool thread_pool_construct(void) {
  return true; // a standalone: internal setup only, no concern bit to register
}

ExtensionInterface g_extension_thread_pool = {
    "thread_pool",
    {2, 0, 0},
    thread_pool_construct,
    NULL,
    NULL,
    NULL,
    NULL,
    ThreadPool_GetSpecificAPI,
};
