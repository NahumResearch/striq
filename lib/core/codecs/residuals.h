#ifndef STRIQ_RESIDUALS_H
#define STRIQ_RESIDUALS_H

/*
 * Residual compression for PLA columns.
 *
 * Format: resid_data = [flag_byte] [payload...]
 *
 * flag_byte values:
 *   0x00 — RLE + zigzag-varint int32
 *   0x10 — RLE + packed int8
 *   0x20 — RLE + packed int16
 */

#include "../types.h"
#include <stdint.h>
#include <stddef.h>

#define RESID_FLAG_RLE      0x00u
#define RESID_FLAG_RLE_I8   0x10u
#define RESID_FLAG_RLE_I16  0x20u

/* Conservative upper bound on encoded size. */
static inline size_t residuals_encode_bound(size_t n)
{
    return 1u + n * 10u + 16u;
}

/*
 * Encode with RLE zero-run compression + zigzag-varint.
 * Raw payload only (no flag byte).
 */
striq_status_t residuals_encode_rle(
    const int64_t *residuals, size_t n,
    uint8_t *out, size_t cap, size_t *out_len);

/*
 * Auto-select the smallest encoding:
 *   - Inspect max |residual| -> choose int8/int16/int32 width
 *   - RLE encode with appropriate quantization
 * Writes flag byte + payload into out.
 */
striq_status_t residuals_encode_auto(
    const int64_t *residuals, size_t n,
    uint8_t *out, size_t cap, size_t *out_len);

/*
 * Decode residuals. Reads flag byte and dispatches to correct decoder.
 * out must hold at least n int64_t values.
 */
striq_status_t residuals_decode(
    const uint8_t *in, size_t in_len,
    int64_t *out, size_t n);

#endif /* STRIQ_RESIDUALS_H */
