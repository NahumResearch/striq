#ifndef STRIQ_BENCH_H
#define STRIQ_BENCH_H

/*
 * bench.h — timing macros and result struct for the STRIQ benchmark harness.
 */

#include <stdint.h>
#include <stddef.h>

typedef struct {
    char     codec[32];
    char     dataset[64];
    char     column[64];
    uint32_t N;
    uint64_t raw_bytes;
    uint64_t compressed_bytes;
    double   ratio;
    double   encode_ms;
    double   decode_ms;
    double   encode_mbs;
    double   decode_mbs;
    double   max_error;
    double   query_mean_us;
    double   query_ds_us;
    double   query_gorilla_us;
    double   query_lz4_us;
    double   query_zstd_us;
    double   query_min_us;
    double   query_max_us;
    double   query_sum_us;
    double   query_count_us;
    double   query_var_us;
    double   query_where_us;
    double   query_value_at_us;
    double   query_scan_us;
} bench_result_t;

#define BENCH_MAX_RESULTS 4096

typedef struct {
    bench_result_t rows[BENCH_MAX_RESULTS];
    int            count;
} bench_results_t;

static inline void bench_results_add(bench_results_t *rs, const bench_result_t *r)
{
    if (rs->count < BENCH_MAX_RESULTS)
        rs->rows[rs->count++] = *r;
}

#endif /* STRIQ_BENCH_H */
