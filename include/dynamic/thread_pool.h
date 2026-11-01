#ifndef CRUX_DYNAMIC_THREAD_POOL_H
#define CRUX_DYNAMIC_THREAD_POOL_H

#include "engine.h"
#include "package_types.h"
#include "thread_pool_api.h"

#ifdef __cplusplus
extern "C" {
#endif

// The dynamic rendering of the thread_pool contract: every call dispatches
// through the engine's registry channel — this package's served ThreadPoolAPI,
// selected by the package's generated dispatch id (PACKAGE_THREAD_POOL, the
// generated package_types.h). crux_engine_api is the module-global pointer
// defined by the install TU the DYNAMIC dialect compiles in and installed by
// the target's Construct (the same pattern as the other packages' dynamic
// renderings).
static inline CruxThreadPool *ThreadPool_Create(int workerCount) {
  return ((ThreadPoolAPI *)crux_engine_api->GetExtensionAPI(PACKAGE_THREAD_POOL))->Create(workerCount);
}
static inline void ThreadPool_Destroy(CruxThreadPool *pool) {
  ((ThreadPoolAPI *)crux_engine_api->GetExtensionAPI(PACKAGE_THREAD_POOL))->Destroy(pool);
}
static inline void ThreadPool_Submit(CruxThreadPool *pool, void (*fn)(void *arg), void *arg) {
  ((ThreadPoolAPI *)crux_engine_api->GetExtensionAPI(PACKAGE_THREAD_POOL))->Submit(pool, fn, arg);
}
static inline void ThreadPool_WaitAll(CruxThreadPool *pool) {
  ((ThreadPoolAPI *)crux_engine_api->GetExtensionAPI(PACKAGE_THREAD_POOL))->WaitAll(pool);
}
static inline int ThreadPool_WorkerCount(CruxThreadPool *pool) {
  return ((ThreadPoolAPI *)crux_engine_api->GetExtensionAPI(PACKAGE_THREAD_POOL))->WorkerCount(pool);
}

#ifdef __cplusplus
}
#endif

#endif
