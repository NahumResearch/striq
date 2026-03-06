#ifndef STRIQ_DECIMAL_H
#define STRIQ_DECIMAL_H

/*
 * CODEC_DECIMAL — lossless compression for decimal-origin sensor data.
 *
 * Inspired by ALP (SIGMOD 2024): real-world sensor data is almost always
 * decimal-origin (stored as doubles but printed as "23.7 °C", "230.1 V").
 *
 * Block format:
 *   [48B] stats header    — identical to RAW_STATS; O(1) queries via raw_stats_parse_hdr
 *   [ 1B] d_exp           — decimal exponent 0..7  (v_int = round(v × 10^d))
 *   [ 1B] delta_bytes     — bytes per zigzag delta (1, 2, or 4)
 *   [ 8B] base_int64      — first value as scaled integer (little-endian)
 *   [(N-1) × delta_bytes] — zigzag-encoded consecutive deltas (little-endian)
 *
 * Space:  58 + (N-1) × delta_bytes
 * vs raw: 48 + N × 8
 * Savings (N=4096, delta_bytes=1): 4153 vs 32816 → ~8× compression
 */

#include "../types.h"
#include "raw_stats.h"
#include <stdbool.h>

#define DECIMAL_FIXED_HDR_BYTES  10u   /* d_exp(1) + delta_bytes(1) + base(8) */
#define DECIMAL_TOTAL_HDR_BYTES  (RAW_STATS_HDR_SIZE + DECIMAL_FIXED_HDR_BYTES)

/*
 * Detect whether data[0..n-1] has decimal origin for any exponent 0..7.
 * On success writes out_d_exp and out_delta_bytes (1, 2, or 4).
 * n may be a small sample (e.g. 256) for fast routing.
 */
bool decimal_detect(
    const double *data,
    uint32_t      n,
    uint8_t      *out_d_exp,
    uint8_t      *out_delta_bytes
);

/*
 * Encode data[0..n-1] into out[0..out_cap).
 * Returns bytes written, or 0 on error (not decimal-origin or cap too small).
 */
size_t decimal_encode(
    const double *data,
    uint32_t      n,
    uint8_t      *out,
    size_t        out_cap
);

/*
 * Decode a DECIMAL block into out[0..n-1].
 */
striq_status_t decimal_decode(
    const uint8_t *src,
    size_t         src_len,
    double        *out,
    size_t         n
);

#endif /* STRIQ_DECIMAL_H */
