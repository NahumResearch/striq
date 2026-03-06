#ifndef STRIQ_CHEBYSHEV_H
#define STRIQ_CHEBYSHEV_H

/*
 * Chebyshev-3 polynomial fitting for STRIQ segments.
 *
 * A cubic Chebyshev series f(u) = c0*T0(u) + c1*T1(u) + c2*T2(u) + c3*T3(u),
 * u ∈ [-1,1], fitted via discrete projection.  On smooth curves (e.g. sine)
 * one Chebyshev segment covers 5-20× more points than one linear segment.
 *
 * Segment serialisation (CODEC_PLA_CHEB, 34 bytes per segment):
 *   c0 f64 (8) + c1 f64 (8) + c2 f64 (8) + c3 f64 (8) + length u16 (2) = 34
 *
 * Algebraic query properties (exact, O(1)):
 *   MEAN     = c0
 *   SUM      = c0 * length
 *   VARIANCE = (c1² + c2² + c3²) / 2  (Parseval)
 *   TREND    = c1  (positive = increasing)
 */

#include "../types.h"
#include <stddef.h>

/*
 * Fit a Chebyshev-3 polynomial to `count` values within epsilon.
 *
 * Returns STRIQ_OK if all `count` points fit within `epsilon_b`.
 * Returns STRIQ_ERR_CODEC if the fit error exceeds epsilon_b.
 * Writes coefficients c[0..3] on success.
 */
striq_status_t cheb3_fit(
    const double *values,
    size_t        count,
    double        epsilon_b,
    double        c[4]
);

/*
 * Evaluate Chebyshev series at normalised u ∈ [-1,1] using Clenshaw recurrence.
 */
double cheb3_eval(const double c[4], double u);

/*
 * Evaluate at all `length` points j=0..length-1, writing to out_values.
 */
void cheb3_eval_range(const double c[4], uint16_t length, double *out_values);

/*
 * Find the maximum window size M (M ≥ min_len, M ≤ max_len) such that
 * cheb3_fit(values+start, M, eps, c) succeeds.
 * Returns 0 if even min_len points don't fit.
 * On success, writes the accepted coefficients to c[4].
 */
size_t cheb3_find_max_length(
    const double *values,
    size_t        start,
    size_t        min_len,
    size_t        max_len,
    double        epsilon_b,
    double        c[4]
);

/*
 * Read a Chebyshev segment from a 34-byte buffer.
 */
static inline void cheb_seg_read(const uint8_t *src,
                                  double c[4], uint16_t *length)
{
    __builtin_memcpy(c,      src,      32);
    __builtin_memcpy(length, src + 32, 2);
}

/*
 * Write a Chebyshev segment to a 34-byte buffer.
 */
static inline void cheb_seg_write(uint8_t *dst,
                                   const double c[4], uint16_t length)
{
    __builtin_memcpy(dst,      c,       32);
    __builtin_memcpy(dst + 32, &length, 2);
}

#define CHEB_SEG_BYTES 34u

#endif /* STRIQ_CHEBYSHEV_H */
