#ifndef STRIQ_MEMSTORE_H
#define STRIQ_MEMSTORE_H

/*
 * memstore — in-RAM ring buffer of compressed blocks.
 *
 * Blocks are stored as opaque byte blobs (same format as on disk).
 * When the ring is full or memory limit is exceeded, the oldest block
 * is evicted and, if eviction_cb is set, the callback is called so
 * that the caller can persist the block to a cold file.
 *
 * The memstore implements block_provider so the query engine works
 * identically for warm and cold data.
 */

#include "../core/block_provider.h"

typedef struct striq_memstore striq_memstore_t;

typedef struct {
    double   epsilon_b;
    uint32_t max_blocks;       /* 0 = 256 */
    uint32_t max_memory_mb;    /* 0 = 16 MB */

    /*
     * Called before a block is evicted (data pointer still valid at call time).
     * col_stats may be NULL if not provided at push time.
     * Set to NULL to ignore evictions.
     */
    void (*eviction_cb)(
        const uint8_t           *data,
        uint32_t                 size,
        const striq_block_index_t *idx,
        const striq_col_stats_t  *col_stats,
        uint32_t                  num_cols,
        void                     *user);
    void *eviction_ctx;
} striq_memstore_opts_t;

striq_memstore_t *memstore_create(
    const striq_col_schema_t *col_schemas,
    uint32_t                  num_cols,
    const striq_memstore_opts_t *opts);

void memstore_destroy(striq_memstore_t *m);

/*
 * Push a completed block into the ring.
 * data / size: raw block bytes (will be memcpy'd into owned storage).
 * idx:         block index entry (timestamps, num_rows, etc.).
 * col_stats:   optional per-column stats; pass NULL if unavailable.
 *
 * If pushing would exceed max_blocks or max_memory_mb, one or more
 * old blocks are evicted first.
 */
striq_status_t memstore_push_block(
    striq_memstore_t         *m,
    const uint8_t            *data,
    uint32_t                  size,
    const striq_block_index_t *idx,
    const striq_col_stats_t  *col_stats);

/*
 * Returns a pointer to the embedded block_provider.
 * Valid for the lifetime of the memstore.
 */
striq_block_provider_t *memstore_provider(striq_memstore_t *m);

typedef struct {
    uint32_t num_blocks;
    uint64_t total_bytes;
    int64_t  ts_oldest;   /* INT64_MIN if empty */
    int64_t  ts_newest;   /* INT64_MIN if empty */
} striq_memstore_info_t;

striq_status_t memstore_info(striq_memstore_t *m, striq_memstore_info_t *out);

#endif /* STRIQ_MEMSTORE_H */
