#ifndef STRIQ_QUANT_H
#define STRIQ_QUANT_H

/*
 * CODEC_QUANT8 / CODEC_QUANT16 — lossy ε-bounded quantization codecs.
 *
 * For noisy continuous signals where epsilon > 0.  Uses uniform scalar
 * quantization: the range [min, max] is divided into QMAX equal steps.
 *
 * Block format:
 *   [48B] stats header  — identical to RAW_STATS; O(1) queries via raw_stats_parse_hdr
 *   [ 8B] range_min     — double, little-endian
 *   [ 8B] range_max     — double, little-endian
 *   [N × width_bytes]   — quantized samples (uint8 or uint16, little-endian)
 *
 * Encoding: q = clamp(round((v - min) / (max - min) × QMAX), 0, QMAX)
 * Decoding: v ≈ min + q × (max - min) / QMAX
 * Max error: (max - min) / QMAX
 *
 * Router gates (require epsilon > 0):
 *   QUANT16: (max-min) / 65535 ≤ ε  →  4× vs raw doubles
 *   QUANT8:  (max-min) / 255   ≤ ε  →  8× vs raw doubles
 */

#include "../types.h"
#include "raw_stats.h"

#define QUANT_RANGE_HDR_BYTES 16u   /* range_min(8) + range_max(8) */
#define QUANT_TOTAL_HDR_BYTES (RAW_STATS_HDR_SIZE + QUANT_RANGE_HDR_BYTES)

/*
 * Encode data[0..n-1] with the given bit width (8 or 16).
 * Returns bytes written, or 0 on error.
 */
size_t quant_encode(
    const double *data,
    uint32_t      n,
    uint8_t       width_bits,
    uint8_t      *out,
    size_t        out_cap
);

/*
 * Decode a QUANT block into out[0..n-1].
 */
striq_status_t quant_decode(
    const uint8_t *src,
    size_t         src_len,
    uint8_t        width_bits,
    double        *out,
    size_t         n
);

#endif /* STRIQ_QUANT_H */
