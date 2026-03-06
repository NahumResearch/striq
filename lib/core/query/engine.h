#ifndef STRIQ_ENGINE_H
#define STRIQ_ENGINE_H

/*
 * Query engine — operates on any striq_block_provider_t.
 *
 * Supported queries:
 *   mean()       — algebraic on PLA segments, decompress fallback
 *   count()      — from block index
 *   mean_where() — algebraic WHERE predicate
 *   min/max()    — algebraic on PLA, decompress fallback
 *   variance()   — Welford merge across blocks; Parseval O(1) for Chebyshev
 *   downsample() — evaluate Base at N equidistant points (zero decompression)
 *   value_at()   — retrieve column values at a specific timestamp
 *   scan()       — extract rows for a time range, one or more columns
 */

#include "../types.h"
#include "../block_provider.h"

typedef struct {
    striq_block_provider_t *provider;
} striq_query_engine_t;

striq_status_t engine_init(
    striq_query_engine_t   *e,
    striq_block_provider_t *provider
);

striq_status_t engine_query_mean(
    striq_query_engine_t *e,
    const char           *col_name,
    int64_t               ts_from,
    int64_t               ts_to,
    striq_query_result_t *out
);

striq_status_t engine_query_count(
    striq_query_engine_t *e,
    int64_t               ts_from,
    int64_t               ts_to,
    uint64_t             *out
);

striq_status_t engine_query_mean_where(
    striq_query_engine_t *e,
    const char           *col_name,
    int64_t               ts_from,
    int64_t               ts_to,
    double                threshold,
    striq_cmp_t           cmp,
    striq_query_result_t *out
);

striq_status_t engine_query_min(
    striq_query_engine_t *e,
    const char           *col_name,
    int64_t               ts_from,
    int64_t               ts_to,
    striq_query_result_t *out
);

striq_status_t engine_query_max(
    striq_query_engine_t *e,
    const char           *col_name,
    int64_t               ts_from,
    int64_t               ts_to,
    striq_query_result_t *out
);

striq_status_t engine_query_variance(
    striq_query_engine_t *e,
    const char           *col_name,
    int64_t               ts_from,
    int64_t               ts_to,
    striq_query_result_t *out
);


striq_status_t engine_query_downsample(
    striq_query_engine_t *e,
    const char           *col_name,
    int64_t               ts_from,
    int64_t               ts_to,
    uint32_t              n_points,
    double               *out_values,
    int64_t              *out_ts
);

striq_status_t engine_query_value_at(
    striq_query_engine_t *e,
    const uint32_t       *col_indices,
    uint32_t              n_cols,
    int64_t               timestamp_ns,
    double               *out_values,
    double               *out_errors
);

striq_status_t engine_query_scan(
    striq_query_engine_t *e,
    const uint32_t       *col_indices,
    uint32_t              n_cols,
    int64_t               ts_from,
    int64_t               ts_to,
    double               *out_values,
    int64_t              *out_timestamps,
    uint32_t              max_rows,
    uint32_t             *out_num_rows
);

#endif /* STRIQ_ENGINE_H */
