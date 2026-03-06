#include "pla.h"
#include "chebyshev.h"
#include "residuals.h"
#include <math.h>
#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>

static inline void lin_seg_write(uint8_t *dst, double slope, double offset,
                                  uint16_t length)
{
    memcpy(dst,      &slope,  8);
    memcpy(dst + 8,  &offset, 8);
    memcpy(dst + 16, &length, 2);
}

static void linear_to_cheb(double slope, double offset, uint16_t L,
                             double c[4])
{
    if (L <= 1) {
        c[0] = offset;
        c[1] = c[2] = c[3] = 0.0;
    } else {
        double half = slope * (double)(L - 1) / 2.0;
        c[0] = offset + half;
        c[1] = half;
        c[2] = 0.0;
        c[3] = 0.0;
    }
}

static double compute_epsilon(const double *values, size_t n)
{
    double mn = values[0], mx = values[0];
    for (size_t i = 1; i < n; i++) {
        if (values[i] < mn) mn = values[i];
        if (values[i] > mx) mx = values[i];
    }
    double range = mx - mn;
    return (range < 1e-12) ? 1e-6 : 0.001 * range;
}

/*
 * Internal structure for one collected segment (before serialisation).
 */
typedef struct {
    uint8_t  kind;       /* 0=linear, 1=chebyshev */
    uint16_t length;
    double   params[4];  /* linear: [slope,offset,0,0]; cheb: [c0,c1,c2,c3] */
} raw_seg_t;

striq_status_t pla_encode(
    const double *values,
    size_t        n,
    double        epsilon_b,
    uint32_t      cheb_threshold,
    uint8_t      *seg_buf,
    size_t        seg_cap,
    size_t       *seg_count,
    bool         *used_cheb,
    uint8_t      *resid_buf,
    size_t        resid_cap,
    size_t       *resid_len)
{
    if (!values || !seg_buf || !seg_count || !resid_buf || !resid_len)
        return STRIQ_ERR_PARAM;
    if (n == 0) {
        *seg_count = 0; *resid_len = 0;
        if (used_cheb) *used_cheb = false;
        return STRIQ_OK;
    }

    if (epsilon_b <= 0.0)
        epsilon_b = compute_epsilon(values, n);

    size_t max_segs = n + 1;
    raw_seg_t *segs = malloc(max_segs * sizeof(raw_seg_t));
    if (!segs) return STRIQ_ERR_MEMORY;

    int64_t *resid_tmp = malloc(n * sizeof(int64_t));
    if (!resid_tmp) { free(segs); return STRIQ_ERR_MEMORY; }

    size_t sc         = 0;  /* segment count */
    size_t rpos       = 0;  /* residual write position */
    size_t seg_start  = 0;
    bool   any_cheb   = false;

    double slope_min = -1e300, slope_max = 1e300;
    double seg_offset = values[0];

#define EMIT_LINEAR(end_excl) do {                                           \
    uint16_t _L = (uint16_t)((end_excl) - seg_start);                       \
    double   _s = (slope_min > 1e299) ? 0.0                                 \
                : (slope_min + slope_max) / 2.0;                             \
    segs[sc].kind       = 0;                                                  \
    segs[sc].length     = _L;                                                 \
    segs[sc].params[0]  = _s;                                                 \
    segs[sc].params[1]  = seg_offset;                                         \
    segs[sc].params[2]  = 0.0;                                                \
    segs[sc].params[3]  = 0.0;                                                \
    sc++;                                                                     \
    for (size_t _j = seg_start; _j < (end_excl); _j++) {                    \
        double _approx = seg_offset + _s * (double)(_j - seg_start);        \
        resid_tmp[rpos++] = (int64_t)round((values[_j] - _approx) * 1e6);  \
    }                                                                         \
} while (0)

#define EMIT_CHEB(seg_len, cheb_c) do {                                       \
    uint16_t _L = (uint16_t)(seg_len);                                        \
    segs[sc].kind      = 1;                                                   \
    segs[sc].length    = _L;                                                  \
    segs[sc].params[0] = (cheb_c)[0];                                         \
    segs[sc].params[1] = (cheb_c)[1];                                         \
    segs[sc].params[2] = (cheb_c)[2];                                         \
    segs[sc].params[3] = (cheb_c)[3];                                         \
    sc++;                                                                      \
    any_cheb = true;                                                           \
    if (_L == 1) {                                                             \
        double _approx = cheb3_eval((cheb_c), 0.0);                           \
        resid_tmp[rpos++] = (int64_t)round((values[seg_start] - _approx)*1e6);\
    } else {                                                                   \
        double _inv = 2.0 / (double)(_L - 1);                                 \
        for (size_t _j = 0; _j < _L; _j++) {                                 \
            double _u = _inv * (double)_j - 1.0;                              \
            double _approx = cheb3_eval((cheb_c), _u);                        \
            resid_tmp[rpos++] = (int64_t)round(                               \
                (values[seg_start + _j] - _approx) * 1e6);                   \
        }                                                                      \
    }                                                                          \
} while (0)

    for (size_t i = 1; i <= n; i++) {
        bool flush = (i == n);

        if (!flush) {
            double dt      = (double)(i - seg_start);
            double s_upper = (values[i] + epsilon_b - seg_offset) / dt;
            double s_lower = (values[i] - epsilon_b - seg_offset) / dt;
            double new_max = (s_upper < slope_max) ? s_upper : slope_max;
            double new_min = (s_lower > slope_min) ? s_lower : slope_min;

            if (new_min <= new_max) {
                slope_min = new_min;
                slope_max = new_max;
                continue;
            }
            /* Cone collapsed at i. Length = i - seg_start. */
        }

        size_t seg_len = (flush ? n : i) - seg_start;

        bool used_cheb_here = false;
        if (cheb_threshold > 0 && seg_len < (size_t)cheb_threshold) {
            size_t avail = n - seg_start;
            double cc[4];
            size_t cheb_len = cheb3_find_max_length(
                values, seg_start, seg_len, avail, epsilon_b, cc);

            if (cheb_len >= seg_len) {
                /* Chebyshev fits at least as many points as linear */
                EMIT_CHEB(cheb_len, cc);
                used_cheb_here = true;
                size_t new_start = seg_start + cheb_len;
                seg_start  = new_start;
                if (new_start < n) {
                    seg_offset = values[new_start];
                }
                slope_min = -1e300;
                slope_max =  1e300;
                i = new_start;  /* loop will i++ → i = new_start + 1 */
                if (flush) break;
                continue;
            }
        }

        if (!used_cheb_here) {
            EMIT_LINEAR((flush ? n : i));
            if (!flush) {
                seg_start  = i;
                seg_offset = values[i];
                slope_min  = -1e300;
                slope_max  =  1e300;
            }
        }
    }

#undef EMIT_LINEAR
#undef EMIT_CHEB

    *seg_count = sc;
    if (used_cheb) *used_cheb = any_cheb;

    size_t seg_bytes_needed = sc * (any_cheb ? CHEB_SEG_BYTES : SEG_LIN_BYTES);
    if (seg_bytes_needed > seg_cap) { free(segs); free(resid_tmp); return STRIQ_ERR_MEMORY; }

    if (!any_cheb) {
        for (size_t s = 0; s < sc; s++) {
            lin_seg_write(seg_buf + s * SEG_LIN_BYTES,
                          segs[s].params[0], segs[s].params[1], segs[s].length);
        }
    } else {
        for (size_t s = 0; s < sc; s++) {
            double cc[4];
            if (segs[s].kind == 0) {
                linear_to_cheb(segs[s].params[0], segs[s].params[1],
                               segs[s].length, cc);
            } else {
                cc[0] = segs[s].params[0];
                cc[1] = segs[s].params[1];
                cc[2] = segs[s].params[2];
                cc[3] = segs[s].params[3];
            }
            cheb_seg_write(seg_buf + s * CHEB_SEG_BYTES, cc, segs[s].length);
        }
    }

    free(segs);

    striq_status_t st = residuals_encode_auto(resid_tmp, rpos,
                                               resid_buf, resid_cap, resid_len);
    free(resid_tmp);
    return st;
}

striq_status_t pla_decode(
    const uint8_t *seg_buf,
    size_t         seg_count,
    bool           is_cheb,
    const uint8_t *resid_buf,
    size_t         resid_len,
    size_t         n,
    double        *out_values)
{
    if (!seg_buf || !resid_buf || !out_values) return STRIQ_ERR_PARAM;
    if (n == 0) return STRIQ_OK;

    int64_t *resid_all = malloc(n * sizeof(int64_t));
    if (!resid_all) return STRIQ_ERR_MEMORY;

    striq_status_t s = residuals_decode(resid_buf, resid_len, resid_all, n);
    if (s != STRIQ_OK) { free(resid_all); return s; }

    size_t vpos = 0;

    if (!is_cheb) {
        for (size_t seg = 0; seg < seg_count; seg++) {
            double slope, offset; uint16_t length;
            lin_seg_read(seg_buf + seg * SEG_LIN_BYTES, &slope, &offset, &length);

            for (uint16_t j = 0; j < length && vpos < n; j++, vpos++) {
                double approx = offset + slope * (double)j;
                out_values[vpos] = approx + (double)resid_all[vpos] * 1e-6;
            }
        }
    } else {
        for (size_t seg = 0; seg < seg_count; seg++) {
            double c[4]; uint16_t length;
            cheb_seg_read(seg_buf + seg * CHEB_SEG_BYTES, c, &length);

            if (length == 1) {
                double approx = cheb3_eval(c, 0.0);
                out_values[vpos] = approx + (double)resid_all[vpos] * 1e-6;
                vpos++;
            } else {
                double inv = 2.0 / (double)(length - 1);
                for (uint16_t j = 0; j < length && vpos < n; j++, vpos++) {
                    double u = inv * (double)j - 1.0;
                    double approx = cheb3_eval(c, u);
                    out_values[vpos] = approx + (double)resid_all[vpos] * 1e-6;
                }
            }
        }
    }

    free(resid_all);
    return STRIQ_OK;
}

striq_status_t pla_query_mean(
    const uint8_t *seg_buf,
    size_t         seg_count,
    double         epsilon_b,
    double        *out_mean,
    double        *out_error)
{
    if (!seg_buf || !out_mean) return STRIQ_ERR_PARAM;
    if (seg_count == 0) return STRIQ_ERR_NOTFOUND;

    double total_sum = 0.0;
    uint64_t total_n = 0;

    for (size_t s = 0; s < seg_count; s++) {
        double slope, offset; uint16_t length;
        lin_seg_read(seg_buf + s * SEG_LIN_BYTES, &slope, &offset, &length);

        double L = (double)length;
        total_sum += L * offset + slope * L * (L - 1.0) / 2.0;
        total_n   += length;
    }

    *out_mean  = total_sum / (double)total_n;
    if (out_error) *out_error = epsilon_b;
    return STRIQ_OK;
}
