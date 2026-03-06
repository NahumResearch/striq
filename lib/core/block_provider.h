#ifndef STRIQ_BLOCK_PROVIDER_H
#define STRIQ_BLOCK_PROVIDER_H

/*
 * block_provider — the hexagonal port between the query engine and storage.
 *
 * Any storage backend (file, memstore ring buffer, mmap, remote) implements
 * this interface. The engine never calls malloc/free or I/O directly.
 *
 * Lifecycle of block data:
 *   get_block_data()  → provider allocates / pins block bytes
 *   release_block()   → provider frees / unpins (may be a no-op for memstore)
 */

#include "types.h"

typedef struct striq_block_provider striq_block_provider_t;

struct striq_block_provider {
    void     *ctx;
    uint32_t  num_blocks;
    uint32_t  num_cols;
    double    epsilon_b;
    double    col_epsilons[STRIQ_MAX_COLS];  /* per-column; 0 = use epsilon_b */
    uint64_t  total_rows;
    striq_col_schema_t *col_schemas;   /* num_cols entries, owned by provider */

    /* Returns block index entry (timestamps, offsets, size). */
    striq_status_t (*get_block_index)(
        void *ctx, uint32_t idx, striq_block_index_t *out);

    /* Provides block bytes. Provider owns the memory until release_block(). */
    striq_status_t (*get_block_data)(
        void *ctx, uint32_t idx, const uint8_t **data, uint32_t *size);

    /* Release block bytes obtained from get_block_data. */
    void (*release_block)(
        void *ctx, uint32_t idx, const uint8_t *data);

    /* Returns pre-computed per-column stats (min/max/sum/count). */
    striq_status_t (*get_col_stats)(
        void *ctx, uint32_t idx, uint32_t col_idx, striq_col_stats_t *out);
};

#endif /* STRIQ_BLOCK_PROVIDER_H */
