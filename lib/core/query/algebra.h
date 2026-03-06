#ifndef STRIQ_ALGEBRA_H
#define STRIQ_ALGEBRA_H

/*
 * Algebraic computations on PLA segment buffers (no decompression needed).
 *
 * Segment layout (18 bytes each):
 *   double slope  (8B) + double offset (8B) + uint16_t length (2B)
 *
 * Formulas:
 *   full segment sum  = offset * L + slope * L * (L-1) / 2
 *   full segment mean = offset + slope * (L-1) / 2
 *
 *   partial sum [local_start, local_end]:
 *     n = local_end - local_start + 1
 *     sum = offset*n + slope*(n*local_start + n*(n-1)/2)
 *
 *   segment min/max (monotone segment):
 *     slope >= 0 → min = offset,          max = offset + slope*(L-1)
 *     slope <  0 → min = offset+slope*(L-1), max = offset
 */

#include "../types.h"

/*
 * Compute mean and count from raw segment bytes (18 bytes per segment).
 * out_mean is the algebraic mean; out_error is epsilon_b.
 */
striq_status_t algebra_mean(
    const uint8_t *seg_buf,
    size_t         seg_count,
    double         epsilon_b,
    double        *out_mean,
    uint64_t      *out_count,
    double        *out_error
);

/*
 * Population variance of values y[i] = offset + slope*i for i in [0, L-1].
 *   Var = slope² * (L²-1) / 12
 */
double algebra_linear_variance(double slope, uint32_t length);

/*
 * Closed-form sum for rows [local_start, local_end] within ONE segment.
 *   n = local_end - local_start + 1
 *   sum = offset*n + slope*(n*local_start + n*(n-1)/2)
 *
 * local_start and local_end are 0-based offsets within the segment.
 * Returns the sum as a plain double (no error return — pure math).
 */
double algebra_partial_sum(
    double   slope,
    double   offset,
    uint32_t local_start,
    uint32_t local_end
);

/*
 * Sum and count of rows in [0, length-1] where `offset + slope*t OP threshold`.
 *
 * For slope != 0, computes t_cross = (threshold - offset) / slope and
 * splits the segment.  For slope == 0, the answer is all-or-nothing.
 *
 * offset_in_seg: 0-based starting offset within the original segment
 * (used when the segment is clipped to a time range).  Pass 0 for full segments.
 */
striq_status_t algebra_sum_where(
    double      slope,
    double      offset,
    uint16_t    length,
    double      threshold,
    striq_cmp_t cmp,
    double     *out_sum,
    uint64_t   *out_count
);

striq_status_t algebra_min(
    const uint8_t        *seg_buf,
    size_t                seg_count,
    double                epsilon_b,
    striq_query_result_t *out
);

striq_status_t algebra_max(
    const uint8_t        *seg_buf,
    size_t                seg_count,
    double                epsilon_b,
    striq_query_result_t *out
);

/*
 * These operate on CODEC_PLA_CHEB buffers: 34 bytes per segment,
 * format c0(8)+c1(8)+c2(8)+c3(8)+length(2).
 *
 * Key identities (exact, O(1) per segment):
 *   MEAN     = c0
 *   SUM      = c0 * length
 *   VARIANCE = (c1² + c2² + c3²) / 2  (Parseval)
 *   TREND    = c1  (positive = increasing)
 */

striq_status_t algebra_cheb_mean(
    const uint8_t *seg_buf,
    size_t         seg_count,
    double        *out_mean,
    uint64_t      *out_count
);

/* Sum = c0 * length, summed across all segments. */
striq_status_t algebra_cheb_sum(
    const uint8_t *seg_buf,
    size_t         seg_count,
    double        *out_sum,
    uint64_t      *out_count
);

double algebra_cheb_variance(const double c[4]);

striq_status_t algebra_cheb_min_max(
    const uint8_t *seg_buf,
    size_t         seg_count,
    double         epsilon_b,
    double        *out_min,
    double        *out_max
);

/*
 * Partial sum over rows [local_start, local_end] within ONE Chebyshev segment.
 * Uses numerical integration via point evaluation (Clenshaw at each point).
 */
double algebra_cheb_partial_sum(
    const double c[4],
    uint16_t     length,
    uint32_t     local_start,
    uint32_t     local_end
);

#endif /* STRIQ_ALGEBRA_H */
