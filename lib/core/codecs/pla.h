#ifndef STRIQ_PLA_H
#define STRIQ_PLA_H

/*
 * Piecewise Linear Approximation (PLA) encoder/decoder.
 *
 * Segment formats:
 *   CODEC_PLA_LINEAR — 18 bytes each: slope(f64) + offset(f64) + length(u16)
 *   CODEC_PLA_CHEB   — 34 bytes each: c0(f64)+c1(f64)+c2(f64)+c3(f64)+length(u16)
 *
 * When cheb_threshold > 0, the encoder tries Chebyshev-3 fitting when the
 * Shrinking Cone produces a short segment (length < cheb_threshold).
 * If Chebyshev fits more points, it emits a 34-byte Chebyshev segment and
 * sets *used_cheb = true.  All segments in the output are then 34-byte Cheb
 * format (linear segments converted to Cheb coefficients with c2=c3=0).
 */

#include "../types.h"
#include <stdbool.h>
#include <string.h>

#define SEG_LIN_BYTES 18u

static inline void lin_seg_read(const uint8_t *src,
                                 double *slope, double *offset,
                                 uint16_t *length)
{
    memcpy(slope,  src,      8);
    memcpy(offset, src + 8,  8);
    memcpy(length, src + 16, 2);
}

/*
 * Encode `n` doubles.
 *
 * cheb_threshold: try Chebyshev when segment length < this value; 0 = no Cheb.
 * seg_buf:        output buffer for segments.
 * seg_count:      number of segments written.
 * used_cheb:      true if any Chebyshev segment was used (output is 34-byte format).
 * resid_buf/len:  compressed residuals.
 */
striq_status_t pla_encode(
    const double  *values,
    size_t         n,
    double         epsilon_b,
    uint32_t       cheb_threshold,
    uint8_t       *seg_buf,
    size_t         seg_cap,
    size_t        *seg_count,
    bool          *used_cheb,
    uint8_t       *resid_buf,
    size_t         resid_cap,
    size_t        *resid_len
);

/*
 * Decode `n` values.
 * is_cheb: true if seg_buf contains 34-byte Chebyshev segments.
 */
striq_status_t pla_decode(
    const uint8_t *seg_buf,
    size_t         seg_count,
    bool           is_cheb,
    const uint8_t *resid_buf,
    size_t         resid_len,
    size_t         n,
    double        *out_values
);

/*
 * Compute mean from linear PLA segments (18-byte format, algebraic, O(K)).
 */
striq_status_t pla_query_mean(
    const uint8_t *seg_buf,
    size_t         seg_count,
    double         epsilon_b,
    double        *out_mean,
    double        *out_error
);

#endif /* STRIQ_PLA_H */
