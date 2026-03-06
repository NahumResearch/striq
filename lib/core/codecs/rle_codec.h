#ifndef STRIQ_RLE_CODEC_H
#define STRIQ_RLE_CODEC_H

/*
 * rle_codec.h — Value-RLE codec for discrete/step signals.
 *
 * Designed for columns with ≤64 unique values (step functions, enum-like
 * sensor readings, Sub_metering etc.). The router sends such columns to
 * CODEC_RLE instead of PLA. RLE is superior to PLA for step-function signals.
 *
 * Block format:
 *   [1 byte]  num_unique  (1–64)
 *   [8 × num_unique bytes]  value_table  (IEEE-754 doubles)
 *   [4 bytes] num_runs     (uint32_t LE)
 *   Per run (packed as two varints):
 *     [1–2 bytes]  value_index  (varint, 0–63)
 *     [1–5 bytes]  run_length   (varint, 1..2^32-1)
 *
 * Algebraic queries on RLE are O(R), R = num_runs — typically very small.
 */

#include "../types.h"

/* Maximum unique values supported by this codec (Phase 6: raised from 16). */
#define RLE_MAX_UNIQUE 64

/*
 * Encode values[0..n-1] into dst[0..dst_cap-1].
 * Returns STRIQ_ERR_CODEC if more than RLE_MAX_UNIQUE distinct values found.
 * out_len: bytes written.
 */
striq_status_t rle_encode(
    const double *values,
    size_t        n,
    uint8_t      *dst,
    size_t        dst_cap,
    size_t       *out_len
);

/*
 * Decode a RLE block back to values[0..n-1].
 * n must match the original count passed to rle_encode.
 */
striq_status_t rle_decode(
    const uint8_t *src,
    size_t         src_len,
    double        *values,
    size_t         n
);

typedef struct {
    double   mean;
    double   sum;
    double   min;
    double   max;
    double   variance;
    uint64_t count;
} rle_stats_t;

/*
 * Compute mean, sum, min, max, variance from a RLE block without full
 * decompression. The block pointer src must be the raw encoded bytes.
 */
striq_status_t rle_query_stats(
    const uint8_t *src,
    size_t         src_len,
    rle_stats_t   *out
);

/*
 * Count rows WHERE value [cmp] threshold.
 * Uses striq_cmp_t values (GT, GTE, LT, LTE, EQ).
 */
striq_status_t rle_query_count_where(
    const uint8_t *src,
    size_t         src_len,
    double         threshold,
    striq_cmp_t    cmp,
    uint64_t      *out_count
);

#endif /* STRIQ_RLE_CODEC_H */
