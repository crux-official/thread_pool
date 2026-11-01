#ifndef CRUX_STATIC_THREAD_POOL_H
#define CRUX_STATIC_THREAD_POOL_H

#include "thread_pool_types.h"

#ifdef __cplusplus
extern "C" {
#endif

// The static rendering of the thread_pool contract: plain externs, defined by
// the package and linked into the binary (a static consumer pays zero
// indirection and cannot accidentally dispatch — the same shape as the other
// packages' static renderings). The API struct itself lives in
// thread_pool_api.h; this header never includes it.
extern CruxThreadPool *ThreadPool_Create(int workerCount);
extern void ThreadPool_Destroy(CruxThreadPool *pool);
extern void ThreadPool_Submit(CruxThreadPool *pool, void (*fn)(void *arg), void *arg);
extern void ThreadPool_WaitAll(CruxThreadPool *pool);
extern int ThreadPool_WorkerCount(CruxThreadPool *pool);

#ifdef __cplusplus
}
#endif

#endif
