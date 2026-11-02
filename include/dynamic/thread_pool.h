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
static inline void ThreadPool_Submit(CruxThreadPool *pool, void (*func)(void *arg), void *arg) {
  ((ThreadPoolAPI *)crux_engine_api->GetExtensionAPI(PACKAGE_THREAD_POOL))->Submit(pool, func, arg);
}
static inline void ThreadPool_WaitAll(CruxThreadPool *pool) {
  ((ThreadPoolAPI *)crux_engine_api->GetExtensionAPI(PACKAGE_THREAD_POOL))->WaitAll(pool);
}
static inline int ThreadPool_WorkerCount(CruxThreadPool *pool) {
  return ((ThreadPoolAPI *)crux_engine_api->GetExtensionAPI(PACKAGE_THREAD_POOL))->WorkerCount(pool);
}
// (v2, backlog 0039) the batch-dispatch additions.
static inline void ThreadPool_SetMode(CruxThreadPool *pool, TPMode mode) {
  ((ThreadPoolAPI *)crux_engine_api->GetExtensionAPI(PACKAGE_THREAD_POOL))->SetMode(pool, mode);
}
static inline void ThreadPool_SubmitSlot(CruxThreadPool *pool, int slot, void (*func)(void *arg), void *arg) {
  ((ThreadPoolAPI *)crux_engine_api->GetExtensionAPI(PACKAGE_THREAD_POOL))->SubmitSlot(pool, slot, func, arg);
}
static inline CruxThreadPool *ThreadPool_CreateWithAllocator(int workerCount, const TPAllocator *allocator) {
  return ((ThreadPoolAPI *)crux_engine_api->GetExtensionAPI(PACKAGE_THREAD_POOL))
      ->CreateWithAllocator(workerCount, allocator);
}

#ifdef __cplusplus
}
#endif

#endif
