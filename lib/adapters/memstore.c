#include "memstore.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#define MEMSTORE_DEFAULT_MAX_BLOCKS  256u
#define MEMSTORE_DEFAULT_MAX_MB      16u

typedef struct {
    uint8_t              *data;
    uint32_t              size;
    striq_block_index_t   idx;
    striq_col_stats_t     col_stats[STRIQ_MAX_COLS];
    bool                  has_stats;
} memstore_entry_t;

struct striq_memstore {
    memstore_entry_t       *ring;
    uint32_t                max_blocks;
    uint32_t                head;
    uint32_t                count;
    uint64_t                total_bytes;
    uint64_t                max_bytes;

    striq_col_schema_t     *col_schemas;
    uint32_t                num_cols;
    double                  epsilon_b;

    void (*eviction_cb)(const uint8_t *, uint32_t,
                        const striq_block_index_t *,
                        const striq_col_stats_t *, uint32_t, void *);
    void *eviction_ctx;

    striq_block_provider_t  provider;    /* embedded, returned by memstore_provider() */
};

static striq_status_t ms_get_block_index(
    void *ctx, uint32_t idx, striq_block_index_t *out)
{
    striq_memstore_t *m = (striq_memstore_t *)ctx;
    if (idx >= m->count) return STRIQ_ERR_NOTFOUND;
    uint32_t phys = (m->head + idx) % m->max_blocks;
    *out = m->ring[phys].idx;
    return STRIQ_OK;
}

static striq_status_t ms_get_block_data(
    void *ctx, uint32_t idx, const uint8_t **data, uint32_t *size)
{
    striq_memstore_t *m = (striq_memstore_t *)ctx;
    if (idx >= m->count) return STRIQ_ERR_NOTFOUND;
    uint32_t phys = (m->head + idx) % m->max_blocks;
    *data = m->ring[phys].data;
    *size = m->ring[phys].size;
    return STRIQ_OK;
}

static void ms_release_block(void *ctx, uint32_t idx, const uint8_t *data)
{
    (void)ctx; (void)idx; (void)data;
}

static striq_status_t ms_get_col_stats(
    void *ctx, uint32_t idx, uint32_t col_idx, striq_col_stats_t *out)
{
    striq_memstore_t *m = (striq_memstore_t *)ctx;
    if (idx >= m->count) return STRIQ_ERR_NOTFOUND;
    uint32_t phys = (m->head + idx) % m->max_blocks;
    if (!m->ring[phys].has_stats || col_idx >= m->num_cols)
        return STRIQ_ERR_NOTFOUND;
    *out = m->ring[phys].col_stats[col_idx];
    return STRIQ_OK;
}

static void ms_update_provider(striq_memstore_t *m)
{
    m->provider.num_blocks = m->count;
}

static void evict_one(striq_memstore_t *m)
{
    if (m->count == 0) return;
    uint32_t phys = m->head;
    memstore_entry_t *e = &m->ring[phys];

    if (m->eviction_cb) {
        m->eviction_cb(e->data, e->size, &e->idx,
                       e->has_stats ? e->col_stats : NULL,
                       m->num_cols, m->eviction_ctx);
    }

    m->total_bytes -= e->size;
    free(e->data);
    e->data = NULL;
    e->size = 0;

    m->head  = (m->head + 1) % m->max_blocks;
    m->count--;
    ms_update_provider(m);
}

striq_memstore_t *memstore_create(
    const striq_col_schema_t *col_schemas,
    uint32_t                  num_cols,
    const striq_memstore_opts_t *opts)
{
    if (!col_schemas || num_cols == 0 || num_cols > STRIQ_MAX_COLS) return NULL;

    striq_memstore_t *m = calloc(1, sizeof(*m));
    if (!m) return NULL;

    uint32_t max_blocks = (opts && opts->max_blocks > 0)
                        ? opts->max_blocks : MEMSTORE_DEFAULT_MAX_BLOCKS;
    uint32_t max_mb     = (opts && opts->max_memory_mb > 0)
                        ? opts->max_memory_mb : MEMSTORE_DEFAULT_MAX_MB;

    m->ring = calloc(max_blocks, sizeof(memstore_entry_t));
    if (!m->ring) { free(m); return NULL; }

    m->col_schemas = malloc(num_cols * sizeof(striq_col_schema_t));
    if (!m->col_schemas) { free(m->ring); free(m); return NULL; }
    memcpy(m->col_schemas, col_schemas, num_cols * sizeof(striq_col_schema_t));

    m->max_blocks  = max_blocks;
    m->max_bytes   = (uint64_t)max_mb * 1024u * 1024u;
    m->num_cols    = num_cols;
    m->epsilon_b   = opts ? opts->epsilon_b : 0.0;
    m->head        = 0;
    m->count       = 0;
    m->total_bytes = 0;

    if (opts) {
        m->eviction_cb  = opts->eviction_cb;
        m->eviction_ctx = opts->eviction_ctx;
    }

    m->provider.ctx         = m;
    m->provider.num_blocks  = 0;
    m->provider.num_cols    = num_cols;
    m->provider.epsilon_b   = m->epsilon_b;
    m->provider.total_rows  = 0;
    m->provider.col_schemas = m->col_schemas;
    m->provider.get_block_index = ms_get_block_index;
    m->provider.get_block_data  = ms_get_block_data;
    m->provider.release_block   = ms_release_block;
    m->provider.get_col_stats   = ms_get_col_stats;

    return m;
}

void memstore_destroy(striq_memstore_t *m)
{
    if (!m) return;
    for (uint32_t i = 0; i < m->max_blocks; i++) {
        if (m->ring[i].data) free(m->ring[i].data);
    }
    free(m->ring);
    free(m->col_schemas);
    free(m);
}

striq_status_t memstore_push_block(
    striq_memstore_t         *m,
    const uint8_t            *data,
    uint32_t                  size,
    const striq_block_index_t *idx,
    const striq_col_stats_t  *col_stats)
{
    if (!m || !data || !idx) return STRIQ_ERR_PARAM;

    /* Evict until we have space (both slot and memory) */
    while (m->count >= m->max_blocks ||
           (m->total_bytes + size > m->max_bytes && m->count > 0))
        evict_one(m);

    uint32_t phys = (m->head + m->count) % m->max_blocks;
    memstore_entry_t *e = &m->ring[phys];

    e->data = malloc(size);
    if (!e->data) return STRIQ_ERR_MEMORY;
    memcpy(e->data, data, size);
    e->size      = size;
    e->idx       = *idx;
    e->has_stats = (col_stats != NULL);
    if (col_stats)
        memcpy(e->col_stats, col_stats,
               m->num_cols * sizeof(striq_col_stats_t));

    m->count++;
    m->total_bytes += size;

    m->provider.total_rows += idx->num_rows;
    ms_update_provider(m);

    return STRIQ_OK;
}

striq_block_provider_t *memstore_provider(striq_memstore_t *m)
{
    return m ? &m->provider : NULL;
}

striq_status_t memstore_info(striq_memstore_t *m, striq_memstore_info_t *out)
{
    if (!m || !out) return STRIQ_ERR_PARAM;

    out->num_blocks  = m->count;
    out->total_bytes = m->total_bytes;
    out->ts_oldest   = INT64_MIN;
    out->ts_newest   = INT64_MIN;

    if (m->count > 0) {
        uint32_t phys_oldest = m->head;
        uint32_t phys_newest = (m->head + m->count - 1) % m->max_blocks;
        out->ts_oldest = m->ring[phys_oldest].idx.ts_first;
        out->ts_newest = m->ring[phys_newest].idx.ts_last;
    }
    return STRIQ_OK;
}
