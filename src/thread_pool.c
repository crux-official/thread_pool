// The worker-thread POOL — a thin, platform-agnostic composition on the
// threading PRIMITIVE (the concern this package requires). The logic (the FIFO
// job queue, the batch barrier, the auto-sizing) is shared; the only platform
// branches are (a) the low-level lock/cond (pthread on posix and web, SRWLOCK +
// condition variables on win32) and (b) the barrier's wait strategy (block on
// native, spin on the web main thread — which cannot block: Atomics.wait is
// illegal there, and emscripten's main-thread blocking deadlocks when a worker
// proxies to it).
//
// The workers themselves are the primitive's threads: each is a CruxThread the
// concern's Thread_Create made (pthread on native, an emscripten Web Worker on
// web) running this package's worker loop (drain the queue until shutdown).
// The package never touches the platform thread API directly — it speaks the
// primitive, which is why one source serves every platform.
//
// Sizing (ThreadPool_Create): auto (n <= 0) sizes to the hardware's PHYSICAL
// core count (Thread_Count_Physical — exact on native, an honest estimate on
// web); an explicit n overrides. Either is clamped to the concurrency ceiling
// (Thread_Concurrency). A ceiling of 0 — threading_none, or an Emscripten page
// without SharedArrayBuffer — creates the pool in SERIAL mode: no workers,
// Submit runs the job inline on the submitting thread, WaitAll is a no-op,
// WorkerCount reads 0, and a named log line says so. The degradation is keyed
// off the primitive's concurrency fact, so the package stays platform-agnostic.
//
// The job contract (thread_pool_api.h): a job is pure computation into
// caller-owned memory — no display/input/module/file/logging calls, no engine
// dispatch, and never a proxy of work back to the submitting (main, on web)
// thread. That last clause is what makes the web barrier's spin
// deadlock-free: nothing needs the main thread to respond while it waits.
//
// Relocated (preserved) from the original threading driver's pool (backlog
// 0037, the head-dequeue FIFO fix included) — the driver is now the thin
// primitive; this package is the pool it was.

#include "crux_threading.h" // the PRIMITIVE: Thread_Create/Join + the hardware facts
#include "extension.h"
#include "kernel_types.h"
#include "logging.h"
#include "memory.h"
#include "thread_pool_api.h"

#include <stdbool.h>
#include <stdint.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <pthread.h>
#ifdef __EMSCRIPTEN__
#include <stdatomic.h>
#endif
#endif

// --- The platform lock/cond (the only per-OS sync). posix and web both speak
// pthreads (emscripten pthreads are real threads with the pthread API); win32
// speaks SRWLOCK + condition variables (SRWLOCK is what WaitOnConditionVariable
// pairs with — a CRITICAL_SECTION is not).
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
  WaitOnConditionVariable(cond, lock, INFINITE);
}
static void tp_signal(TPCond *cond) {
  WakeConditionVariable(cond);
}
static void tp_broadcast(TPCond *cond) {
  WakeAllConditionVariable(cond);
}
static void tp_lock_init(TPLock *lock, TPCond *condA, TPCond *condB) {
  InitializeSRWLock(lock);
  InitializeConditionVariable(condA);
  InitializeConditionVariable(condB);
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
static void tp_signal(TPCond *cond) {
  pthread_cond_signal(cond);
}
static void tp_broadcast(TPCond *cond) {
  pthread_cond_broadcast(cond);
}
static void tp_lock_init(TPLock *lock, TPCond *condA, TPCond *condB) {
  pthread_mutex_init(lock, NULL);
  pthread_cond_init(condA, NULL);
  pthread_cond_init(condB, NULL);
}
#endif

// The queue node — allocated in Submit (memory concern), freed by the worker
// after the run; the package owns the node, the caller owns the job's arg.
typedef struct TPJob {
  void (*fn)(void *arg);
  void *arg;
  struct TPJob *next;
} TPJob;

// The completion counter. On web the barrier's spin (on the main thread) reads
// it while the workers write it — a genuine cross-thread fact, so it is atomic
// there. On native the barrier BLOCKS on a condition variable (the lock gives
// the happens-before), so it is a plain counter.
#ifdef __EMSCRIPTEN__
#define TP_DONE_FIELD atomic_uint_fast64_t done
#else
#define TP_DONE_FIELD uint64_t done
#endif

struct CruxThreadPool {
  bool serial;          // the named degradation (ceiling 0): inline execution, no workers
  CruxThread **workers; // the primitive's thread handles (the worker set)
  int workerCount;
  TPJob *head;
  TPJob *tail;
  uint64_t submitted; // the batch counter: the jobs handed to the queue (app-thread fact)
  TP_DONE_FIELD;      // the jobs finished (the barrier's fact)
  TPLock lock;        // the queue's lock (workers + the app thread)
  TPCond workCond;    // a worker wakes for work (or the shutdown)
  TPCond doneCond;    // the native barrier wakes when done meets submitted
                      // (initialized + destroyed on web too, where the barrier
                      // spins on the atomic and this is simply unused)
  bool destroyed;     // the shutdown flag the worker loop exits on
};

// The worker loop: wait for work (or the shutdown), take the OLDEST job off
// the queue, run it, count it done. The job's node is freed after the run — the
// package owns the node's lifetime, the caller owns the job's arg. This runs
// on a primitive thread (the package hands it to Thread_Create as the unit of
// work).
static void tp_worker_main(void *arg) {
  struct CruxThreadPool *pool = (struct CruxThreadPool *)arg;
  for (;;) {
    tp_lock(&pool->lock);
    while (pool->tail == NULL && !pool->destroyed) {
      tp_wait(&pool->workCond, &pool->lock);
    }
    if (pool->destroyed) {
      tp_unlock(&pool->lock);
      return;
    }
    // Dequeue from the HEAD (a true FIFO): the append links old->new, so the
    // tail's next is NULL by construction and popping there would collapse the
    // list (the head-NULL step) and lose every queued job. The head is the
    // oldest job; when the last one is taken, the tail clears with it. (The
    // head-dequeue fix, preserved from the original driver's pool.)
    TPJob *job = pool->head;
    pool->head = job->next;
    if (pool->head == NULL) {
      pool->tail = NULL;
    }
    tp_unlock(&pool->lock);

    job->fn(job->arg);

    tp_lock(&pool->lock);
#ifdef __EMSCRIPTEN__
    // The web barrier spins on the atomic (the main thread never takes the
    // lock in WaitAll); a release-store pairs with its acquire-load.
    atomic_fetch_add_explicit(&pool->done, 1, memory_order_release);
#else
    pool->done++;
    if (pool->done == pool->submitted) {
      tp_broadcast(&pool->doneCond);
    }
#endif
    tp_unlock(&pool->lock);

    Memory_Free(job);
  }
}

CruxThreadPool *ThreadPool_Create(int workerCount) {
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
  tp_lock_init(&pool->lock, &pool->workCond, &pool->doneCond);
  pool->serial = (count == 0);
  pool->workerCount = count;

  if (pool->serial) {
    // The named degradation (the header's ceiling-0 case): inline execution, no
    // workers; one build serves both pages.
    LogWarning("[thread_pool] Create: the concurrency ceiling is 0 - the pool runs SERIAL (jobs run inline; no workers)");
    return pool;
  }

  pool->workers = (CruxThread **)Memory_Calloc((size_t)count, sizeof(CruxThread *));
  if (pool->workers == NULL) {
    Memory_Free(pool);
    LogError("[thread_pool] Create: the worker table failed to allocate");
    return NULL;
  }
  for (int i = 0; i < count; i++) {
    // A worker is a primitive thread running this package's worker loop.
    pool->workers[i] = Thread_Create(tp_worker_main, pool);
    if (pool->workers[i] == NULL) {
      // A refused worker is a pool failure: the cell declared threads and
      // cannot honor them — tear down what exists and report NULL (the call
      // site refuses to boot named; there is no silent serial fallback).
      // Signal under the lock, exactly as Destroy does: a worker parked between
      // its queue check and its wait would otherwise miss an unlocked broadcast
      // and sleep forever, deadlocking the Join below.
      tp_lock(&pool->lock);
      pool->destroyed = true;
      tp_broadcast(&pool->workCond);
      tp_unlock(&pool->lock);
      for (int j = 0; j < i; j++) {
        Thread_Join(pool->workers[j]);
      }
      Memory_Free((void *)pool->workers);
      Memory_Free(pool);
      LogError("[thread_pool] Create: the driver refused worker %d of %d", i + 1, count);
      return NULL;
    }
  }
  return pool;
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
  // The contract says no in-flight batch at Destroy; a node left queued was
  // never run — free it (its fn never ran). A serial pool has none.
  TPJob *job = pool->head;
  while (job != NULL) {
    TPJob *next = job->next;
    Memory_Free(job);
    job = next;
  }
#ifndef _WIN32
  pthread_mutex_destroy(&pool->lock);
  pthread_cond_destroy(&pool->workCond);
  pthread_cond_destroy(&pool->doneCond);
#endif
  if (pool->workers != NULL) {
    Memory_Free((void *)pool->workers);
  }
  Memory_Free(pool);
}

void ThreadPool_Submit(CruxThreadPool *pool, void (*func)(void *arg), void *arg) {
  if (pool == NULL || func == NULL) {
    return;
  }
  if (pool->serial) {
    func(arg); // inline (no workers); the barrier is a no-op
    return;
  }
  // Alloc, not Calloc: every field (fn, arg, next) is set below, so the
  // zero-fill would be pure waste on the per-job hot path.
  TPJob *job = (TPJob *)Memory_Alloc(sizeof *job);
  if (job == NULL) {
    // The batch counter moves only with a queued node — a dropped job must
    // not open the barrier it cannot close.
    LogError("[thread_pool] Submit: the job node failed to allocate (the job drops)");
    return;
  }
  job->fn = func;
  job->arg = arg;
  job->next = NULL;
  tp_lock(&pool->lock);
  pool->submitted++;
  if (pool->tail != NULL) {
    pool->tail->next = job;
  } else {
    pool->head = job;
  }
  pool->tail = job;
  tp_signal(&pool->workCond);
  tp_unlock(&pool->lock);
}

void ThreadPool_WaitAll(CruxThreadPool *pool) {
  if (pool == NULL) {
    return;
  }
  if (pool->serial) {
    return; // the jobs ran inline on Submit; there is nothing to wait for
  }
#ifdef __EMSCRIPTEN__
  // The web main thread CANNOT block (Atomics.wait is illegal there, and
  // emscripten's main-thread blocking deadlocks when a worker proxies to it).
  // So the barrier SPINS on the atomic completion counter (Atomics.load is
  // legal on the main thread). It is deadlock-free BECAUSE of the job
  // contract: a job is pure computation and never proxies work to the main
  // thread, so nothing needs it to respond while it spins. The cost is a
  // briefly unresponsive tab for the batch's duration — right for well-sized
  // jobs; the long-batch escape (a frame-granular drain) is a v2 jobs-system
  // decision.
  const uint64_t target = pool->submitted;
  while (atomic_load_explicit(&pool->done, memory_order_acquire) < target) {
    // spin (the main thread cannot block)
  }
#else
  tp_lock(&pool->lock);
  // Snapshot the batch target under the lock so the native wait matches the web
  // spin (both wait for the work handed to the queue at THIS call, not work a
  // worker submits mid-wait — scoping that is the jobs-system's job, 0038).
  const uint64_t target = pool->submitted;
  while (pool->done < target) {
    tp_wait(&pool->doneCond, &pool->lock);
  }
  tp_unlock(&pool->lock);
#endif
}

int ThreadPool_WorkerCount(CruxThreadPool *pool) {
  return (pool != NULL) ? pool->workerCount : 0;
}

// The served struct: the thread_pool contract, filled with this package's
// function addresses (valid from load, before Construct).
static ThreadPoolAPI g_thread_pool_api = {
    .version = {1, 0, 0},
    .Create = ThreadPool_Create,
    .Destroy = ThreadPool_Destroy,
    .Submit = ThreadPool_Submit,
    .WaitAll = ThreadPool_WaitAll,
    .WorkerCount = ThreadPool_WorkerCount,
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
    {1, 0, 0},
    thread_pool_construct,
    NULL,
    NULL,
    NULL,
    NULL,
    ThreadPool_GetSpecificAPI,
};
