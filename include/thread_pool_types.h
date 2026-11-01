#ifndef CRUX_THREAD_POOL_TYPES_H
#define CRUX_THREAD_POOL_TYPES_H

#ifdef __cplusplus
extern "C" {
#endif

// The opaque pool handle. The pool is a thin composition on the threading
// primitive: a set of worker threads (each a CruxThread the primitive made)
// draining a FIFO job queue, plus a batch barrier (WaitAll). The caller tracks
// the handle it is handed and passes it back; it is never dereferenced. A pool
// whose concurrency ceiling is 0 is created in SERIAL mode (no workers; jobs
// run inline on the submitting thread) and reports a worker count of 0.
typedef struct CruxThreadPool CruxThreadPool;

#ifdef __cplusplus
}
#endif

#endif
