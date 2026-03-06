#include "algebra.h"
#include "../codecs/pla.h"
#include "../codecs/chebyshev.h"
#include <string.h>
#include <math.h>
#include <stdbool.h>

#define SEG_BYTES SEG_LIN_BYTES
#define seg_read  lin_seg_read

striq_status_t algebra_mean(
    const uint8_t *seg_buf,
    size_t         seg_count,
    double         epsilon_b,
    double        *out_mean,
    uint64_t      *out_count,
    double        *out_error)
{
    if (!seg_buf || !out_mean || !out_count || !out_error)
        return STRIQ_ERR_PARAM;
    if (seg_count == 0) return STRIQ_ERR_NOTFOUND;

    double   total_sum = 0.0;
    uint64_t total_n   = 0;

    for (size_t s = 0; s < seg_count; s++) {
        double slope, offset; uint16_t L;
        seg_read(seg_buf + s * SEG_BYTES, &slope, &offset, &L);
        total_sum += offset * (double)L + slope * (double)L * (double)(L - 1) / 2.0;
        total_n   += L;
    }

    if (total_n == 0) return STRIQ_ERR_QUERY;

    *out_mean  = total_sum / (double)total_n;
    *out_count = total_n;
    *out_error = epsilon_b;
    return STRIQ_OK;
}

/*
 * Population variance of values y[i] = offset + slope*i for i in [0, L-1].
 * Derivation: Var = slope² * (L²-1) / 12
 */
double algebra_linear_variance(double slope, uint32_t length)
{
    if (length < 2) return 0.0;
    double L = (double)length;
    return slope * slope * (L * L - 1.0) / 12.0;
}

/*
 * Closed-form sum for rows [local_start, local_end] (0-based within segment):
 *   n = local_end - local_start + 1
 *   t runs from local_start to local_end
 *   sum = Σ(offset + slope * t) for t in [local_start..local_end]
 *       = offset*n + slope*(n*local_start + n*(n-1)/2)
 */
double algebra_partial_sum(
    double   slope,
    double   offset,
    uint32_t local_start,
    uint32_t local_end)
{
    if (local_end < local_start) return 0.0;
    double n = (double)(local_end - local_start + 1);
    double s = (double)local_start;
    return offset * n + slope * (n * s + n * (n - 1.0) / 2.0);
}

/*
 * For a segment of `length` rows starting at t=0:
 *   value(t) = offset + slope * t
 *
 * Find rows where value(t) satisfies `cmp threshold` and compute their sum.
 *
 * Uses t_cross = (threshold - offset) / slope as the real-valued crossing
 * point. Maps this to integer row range and calls algebra_partial_sum.
 */
striq_status_t algebra_sum_where(
    double      slope,
    double      offset,
    uint16_t    length,
    double      threshold,
    striq_cmp_t cmp,
    double     *out_sum,
    uint64_t   *out_count)
{
    if (!out_sum || !out_count) return STRIQ_ERR_PARAM;
    *out_sum = 0.0; *out_count = 0;
    if (length == 0) return STRIQ_OK;

    uint32_t Lm1 = length - 1;

    if (fabs(slope) < 1e-30) {
        bool qualifies = false;
        switch (cmp) {
            case STRIQ_CMP_GT:  qualifies = offset >  threshold; break;
            case STRIQ_CMP_GTE: qualifies = offset >= threshold; break;
            case STRIQ_CMP_LT:  qualifies = offset <  threshold; break;
            case STRIQ_CMP_LTE: qualifies = offset <= threshold; break;
            case STRIQ_CMP_EQ:  qualifies = fabs(offset - threshold) < 1e-9; break;
        }
        if (qualifies) {
            *out_sum   = offset * (double)length;
            *out_count = length;
        }
        return STRIQ_OK;
    }

    /*
     * General case: find the qualifying integer range [q_lo, q_hi] where
     * all rows t in [q_lo, q_hi] satisfy the predicate.
     *
     * t_cross = (threshold - offset) / slope
     *
     * For slope > 0 (increasing): value(t) > threshold ↔ t > t_cross
     * For slope < 0 (decreasing): value(t) > threshold ↔ t < t_cross
     */
    double t_cross = (threshold - offset) / slope;

    /*
     * Compute qualifying range for each comparison.
     * We map to a [lo, hi] half-open interval over the reals, then convert
     * to integers carefully to avoid off-by-one errors.
     *
     * Convention: result_lo_real and result_hi_real are the real-valued
     * bounds (inclusive) of t values that qualify. Rows that qualify are
     * t in [ceil(lo), floor(hi)] intersected with [0, length-1].
     */
    double lo_real, hi_real;   /* real-valued qualifying interval, inclusive */

    switch (cmp) {
        case STRIQ_CMP_GT:
            if (slope > 0.0) { lo_real = t_cross;  hi_real =  1e30; }
            else             { lo_real = -1e30;     hi_real = t_cross; }
            /* strict: exclude exact crossing */
            lo_real += 1e-12;
            hi_real -= 1e-12;
            break;
        case STRIQ_CMP_GTE:
            if (slope > 0.0) { lo_real = t_cross;  hi_real =  1e30; }
            else             { lo_real = -1e30;     hi_real = t_cross; }
            break;
        case STRIQ_CMP_LT:
            if (slope > 0.0) { lo_real = -1e30;     hi_real = t_cross; }
            else             { lo_real = t_cross;  hi_real =  1e30; }
            lo_real += 1e-12;
            hi_real -= 1e-12;
            break;
        case STRIQ_CMP_LTE:
            if (slope > 0.0) { lo_real = -1e30;     hi_real = t_cross; }
            else             { lo_real = t_cross;  hi_real =  1e30; }
            break;
        default: /* EQ: skip (too imprecise algebraically) */
            return STRIQ_OK;
    }

    /* Intersect [lo_real, hi_real] with [0, Lm1] and convert to integers */
    if (lo_real > (double)Lm1 + 0.5) return STRIQ_OK;  /* fully outside */
    if (hi_real < -0.5)              return STRIQ_OK;

    int64_t q_lo_i = (lo_real <= 0.0) ? 0 : (int64_t)ceil(lo_real - 1e-12);
    int64_t q_hi_i = (hi_real >= (double)Lm1 + 0.5) ? (int64_t)Lm1
                   : (int64_t)floor(hi_real + 1e-12);

    if (q_lo_i > q_hi_i) return STRIQ_OK;
    if (q_lo_i < 0) q_lo_i = 0;
    if (q_hi_i > (int64_t)Lm1) q_hi_i = (int64_t)Lm1;

    uint32_t q_lo = (uint32_t)q_lo_i;
    uint32_t q_hi = (uint32_t)q_hi_i;

    *out_sum   = algebra_partial_sum(slope, offset, q_lo, q_hi);
    *out_count = q_hi - q_lo + 1;
    return STRIQ_OK;
}

striq_status_t algebra_min(
    const uint8_t        *seg_buf,
    size_t                seg_count,
    double                epsilon_b,
    striq_query_result_t *out)
{
    if (!seg_buf || !out) return STRIQ_ERR_PARAM;
    if (seg_count == 0)   return STRIQ_ERR_NOTFOUND;

    bool   found   = false;
    double seg_min = 0.0;

    for (size_t s = 0; s < seg_count; s++) {
        double slope, offset; uint16_t L;
        seg_read(seg_buf + s * SEG_BYTES, &slope, &offset, &L);

        double lo, hi;
        if (slope >= 0.0) {
            lo = offset;
            hi = offset + slope * (double)(L - 1);
        } else {
            lo = offset + slope * (double)(L - 1);
            hi = offset;
        }
        (void)hi;

        if (!found || lo < seg_min) {
            seg_min = lo;
            found   = true;
        }
    }

    if (!found) return STRIQ_ERR_NOTFOUND;
    out->value        = seg_min;
    out->error_bound  = epsilon_b;
    out->rows_scanned = 0;
    out->pct_data_read = 0.0;
    return STRIQ_OK;
}

striq_status_t algebra_max(
    const uint8_t        *seg_buf,
    size_t                seg_count,
    double                epsilon_b,
    striq_query_result_t *out)
{
    if (!seg_buf || !out) return STRIQ_ERR_PARAM;
    if (seg_count == 0)   return STRIQ_ERR_NOTFOUND;

    bool   found   = false;
    double seg_max = 0.0;

    for (size_t s = 0; s < seg_count; s++) {
        double slope, offset; uint16_t L;
        seg_read(seg_buf + s * SEG_BYTES, &slope, &offset, &L);

        double hi = (slope >= 0.0) ? offset + slope * (double)(L - 1)
                                   : offset;

        if (!found || hi > seg_max) {
            seg_max = hi;
            found   = true;
        }
    }

    if (!found) return STRIQ_ERR_NOTFOUND;
    out->value         = seg_max;
    out->error_bound   = epsilon_b;
    out->rows_scanned  = 0;
    out->pct_data_read = 0.0;
    return STRIQ_OK;
}

striq_status_t algebra_cheb_mean(
    const uint8_t *seg_buf,
    size_t         seg_count,
    double        *out_mean,
    uint64_t      *out_count)
{
    if (!seg_buf || !out_mean || !out_count) return STRIQ_ERR_PARAM;
    if (seg_count == 0) return STRIQ_ERR_NOTFOUND;

    /* mean = c0 exactly (integral of T_k for k>=1 is 0 over [-1,1]) */
    double   total_sum = 0.0;
    uint64_t total_n   = 0;

    for (size_t s = 0; s < seg_count; s++) {
        double c[4]; uint16_t L;
        cheb_seg_read(seg_buf + s * CHEB_SEG_BYTES, c, &L);
        total_sum += c[0] * (double)L;  /* mean = c0, sum = c0*L */
        total_n   += L;
    }

    if (total_n == 0) return STRIQ_ERR_QUERY;
    *out_mean  = total_sum / (double)total_n;
    *out_count = total_n;
    return STRIQ_OK;
}

striq_status_t algebra_cheb_sum(
    const uint8_t *seg_buf,
    size_t         seg_count,
    double        *out_sum,
    uint64_t      *out_count)
{
    if (!seg_buf || !out_sum || !out_count) return STRIQ_ERR_PARAM;
    if (seg_count == 0) return STRIQ_ERR_NOTFOUND;

    double   total = 0.0;
    uint64_t total_n = 0;

    for (size_t s = 0; s < seg_count; s++) {
        double c[4]; uint16_t L;
        cheb_seg_read(seg_buf + s * CHEB_SEG_BYTES, c, &L);
        total   += c[0] * (double)L;
        total_n += L;
    }

    *out_sum   = total;
    *out_count = total_n;
    return STRIQ_OK;
}

double algebra_cheb_variance(const double c[4])
{
    /* Parseval: var ≈ (c1² + c2² + c3²) / 2 */
    return (c[1]*c[1] + c[2]*c[2] + c[3]*c[3]) / 2.0;
}

striq_status_t algebra_cheb_min_max(
    const uint8_t *seg_buf,
    size_t         seg_count,
    double         epsilon_b,
    double        *out_min,
    double        *out_max)
{
    if (!seg_buf || !out_min || !out_max) return STRIQ_ERR_PARAM;
    if (seg_count == 0) return STRIQ_ERR_NOTFOUND;

    bool   found   = false;
    double gmin    = 0.0, gmax = 0.0;

    /* Chebyshev nodes in [-1,1] for n=3: ±1, ±cos(π/4)=±0.707, 0 */
    static const double probe_u[] = { -1.0, -0.707107, 0.0, 0.707107, 1.0 };
    static const int    n_probes  = 5;

    for (size_t s = 0; s < seg_count; s++) {
        double c[4]; uint16_t L;
        cheb_seg_read(seg_buf + s * CHEB_SEG_BYTES, c, &L);

        for (int p = 0; p < n_probes; p++) {
            double v = cheb3_eval(c, probe_u[p]);
            if (!found || v < gmin) gmin = v;
            if (!found || v > gmax) gmax = v;
            found = true;
        }
    }

    if (!found) return STRIQ_ERR_NOTFOUND;
    *out_min = gmin - epsilon_b;
    *out_max = gmax + epsilon_b;
    return STRIQ_OK;
}

double algebra_cheb_partial_sum(
    const double c[4],
    uint16_t     length,
    uint32_t     local_start,
    uint32_t     local_end)
{
    if (local_end < local_start || length == 0) return 0.0;
    if (local_end >= (uint32_t)length) local_end = (uint32_t)length - 1;

    double sum = 0.0;
    if (length == 1) {
        return cheb3_eval(c, 0.0);
    }

    double inv = 2.0 / (double)(length - 1);
    for (uint32_t j = local_start; j <= local_end; j++) {
        double u = inv * (double)j - 1.0;
        sum += cheb3_eval(c, u);
    }
    return sum;
}
