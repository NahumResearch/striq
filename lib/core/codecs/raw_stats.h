#ifndef STRIQ_RAW_STATS_H
#define STRIQ_RAW_STATS_H

/*
 * raw_stats.h — RAW_STATS codec: uncompressed fallback with precomputed stats.
 *
 * Used when no other codec fits (chaotic signals, very tight epsilon).
 * Provides O(1) algebraic queries (mean/sum/min/max/var) by storing a
 * 48-byte stats header before the raw doubles.
 *
 * Block format:
 *   [8 bytes] sum       (double)
 *   [8 bytes] min       (double)
 *   [8 bytes] max       (double)
 *   [8 bytes] sum_sq    (double)  — for variance: var = sum_sq/N - mean²
 *   [8 bytes] count     (uint64_t)
 *   [8 bytes] nz_count  (uint64_t) — non-zero values (for WHERE != 0)
 *   [N × 8 bytes] raw doubles
 *
 * Query algebra (O(1) per block — reads only the 48-byte header):
 *   mean  = sum / count
 *   var   = sum_sq / count - mean²
 *   min   = min
 *   max   = max
 *
 * WHERE / downsample: reads raw doubles directly (no decompression).
 */

#include "../types.h"
#include "../../platform/simd.h"
#include <string.h>

#define RAW_STATS_HDR_SIZE 48u

static inline void raw_stats_write_hdr(
    uint8_t *out,
    double sum, double min_v, double max_v, double sum_sq,
    uint64_t count, uint64_t nz_count)
{
    uint8_t *p = out;
    memcpy(p, &sum,      8); p += 8;
    memcpy(p, &min_v,    8); p += 8;
    memcpy(p, &max_v,    8); p += 8;
    memcpy(p, &sum_sq,   8); p += 8;
    memcpy(p, &count,    8); p += 8;
    memcpy(p, &nz_count, 8);
}

/* Parsed stats header (in-memory representation). */
typedef struct {
    double   sum;
    double   min;
    double   max;
    double   sum_sq;
    uint64_t count;
    uint64_t nz_count;
} striq_raw_stats_hdr_t;

/*
 * Encode data[0..count-1] into out[].
 * Writes: 48-byte stats header + count×8 raw doubles.
 * Returns total bytes written, or 0 on error (out_capacity too small).
 */
size_t raw_stats_encode(
    const double *data,
    uint32_t      count,
    uint8_t      *out,
    size_t        out_capacity
);

/*
 * Decode a RAW_STATS block: skips the 48-byte header and copies raw doubles
 * into out[0..n-1].
 */
striq_status_t raw_stats_decode(
    const uint8_t *src,
    size_t         src_len,
    double        *out,
    size_t         n
);

/*
 * Parse the 48-byte stats header from a RAW_STATS block.
 * Does NOT read the raw doubles — O(1).
 */
striq_status_t raw_stats_parse_hdr(
    const uint8_t        *src,
    size_t                src_len,
    striq_raw_stats_hdr_t *out
);

#endif /* STRIQ_RAW_STATS_H */
