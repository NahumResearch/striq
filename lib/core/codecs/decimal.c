#include "decimal.h"
#include "zigzag.h"
#include "../../platform/simd.h"
#include <math.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

static const double POW10[8] = {
    1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0, 1000000.0, 10000000.0
};

#define zz_enc zigzag_encode
#define zz_dec zigzag_decode

bool decimal_detect(
    const double *data,
    uint32_t      n,
    uint8_t      *out_d_exp,
    uint8_t      *out_delta_bytes)
{
    if (!data || n == 0 || !out_d_exp || !out_delta_bytes) return false;

    for (uint8_t d = 0; d <= 7; d++) {
        double scale = POW10[d];
        bool   ok    = true;
        uint64_t max_zz = 0;
        int64_t  prev_q = 0;

        for (uint32_t i = 0; i < n && ok; i++) {
            double scaled = data[i] * scale;
            int64_t q = (int64_t)llround(scaled);

            /* Tolerance probe: accepts values with IEEE 754 representation noise
             * (e.g. 23.40000001 stored as float) as valid 1-decimal values.
             * tol = 1e-9 * max(1.0, |v|) ensures relative tolerance for large
             * magnitudes and absolute tolerance for values near 0. */
            double reconstructed = (double)q / scale;
            double abs_v = fabs(data[i]);
            double tol   = 1e-9 * (abs_v > 1.0 ? abs_v : 1.0);
            if (fabs(reconstructed - data[i]) > tol) {
                ok = false;
                break;
            }

            if (i > 0) {
                int64_t  delta = q - prev_q;
                uint64_t zz    = zz_enc(delta);
                if (zz > max_zz) max_zz = zz;
            }
            prev_q = q;
        }

        if (!ok) continue;

        uint8_t db;
        if      (max_zz <= 0xFFU)   db = 1;
        else if (max_zz <= 0xFFFFU) db = 2;
        else                        db = 4;

        *out_d_exp       = d;
        *out_delta_bytes = db;
        return true;
    }
    return false;
}

size_t decimal_encode(
    const double *data,
    uint32_t      n,
    uint8_t      *out,
    size_t        out_cap)
{
    if (!data || !out || n == 0) return 0;

    uint8_t d_exp, delta_bytes;
    if (!decimal_detect(data, n, &d_exp, &delta_bytes)) return 0;

    double   scale = POW10[d_exp];
    int64_t *qs    = (int64_t *)malloc((size_t)n * sizeof(int64_t));
    if (!qs) return 0;

    for (uint32_t i = 0; i < n; i++)
        qs[i] = (int64_t)llround(data[i] * scale);

    uint64_t max_zz = 0;
    for (uint32_t i = 1; i < n; i++) {
        int64_t  delta = qs[i] - qs[i - 1];
        uint64_t zz    = zz_enc(delta);
        if (zz > max_zz) max_zz = zz;
    }
    if      (max_zz <= 0xFFU)   delta_bytes = 1;
    else if (max_zz <= 0xFFFFU) delta_bytes = 2;
    else                        delta_bytes = 4;

    size_t deltas_bytes = (n > 1) ? (size_t)(n - 1) * delta_bytes : 0;
    size_t needed = DECIMAL_TOTAL_HDR_BYTES + deltas_bytes;
    if (out_cap < needed) { free(qs); return 0; }

    double   sum = 0.0, min_v = data[0], max_v = data[0], sum_sq = 0.0;
    uint64_t nz  = 0;
    striq_stats_reduce(data, n, &sum, &min_v, &max_v, &sum_sq, &nz);

    raw_stats_write_hdr(out, sum, min_v, max_v, sum_sq, (uint64_t)n, nz);

    uint8_t *p = out + RAW_STATS_HDR_SIZE;
    *p++ = d_exp;
    *p++ = delta_bytes;
    memcpy(p, &qs[0], 8); p += 8;

    for (uint32_t i = 1; i < n; i++) {
        int64_t  delta = qs[i] - qs[i - 1];
        uint64_t zz    = zz_enc(delta);
        if (delta_bytes == 1) {
            *p++ = (uint8_t)(zz & 0xFF);
        } else if (delta_bytes == 2) {
            p[0] = (uint8_t)(zz & 0xFF);
            p[1] = (uint8_t)((zz >> 8) & 0xFF);
            p += 2;
        } else {
            p[0] = (uint8_t)(zz & 0xFF);
            p[1] = (uint8_t)((zz >> 8) & 0xFF);
            p[2] = (uint8_t)((zz >> 16) & 0xFF);
            p[3] = (uint8_t)((zz >> 24) & 0xFF);
            p += 4;
        }
    }

    free(qs);
    return needed;
}

striq_status_t decimal_decode(
    const uint8_t *src,
    size_t         src_len,
    double        *out,
    size_t         n)
{
    if (!src || !out) return STRIQ_ERR_PARAM;
    if (n == 0) return STRIQ_OK;
    if (src_len < DECIMAL_TOTAL_HDR_BYTES) return STRIQ_ERR_FORMAT;

    const uint8_t *p = src + RAW_STATS_HDR_SIZE;

    uint8_t d_exp       = *p++;
    uint8_t delta_bytes = *p++;

    if (d_exp > 7) return STRIQ_ERR_FORMAT;
    if (delta_bytes != 1 && delta_bytes != 2 && delta_bytes != 4)
        return STRIQ_ERR_FORMAT;

    size_t deltas_bytes = (n > 1) ? (size_t)(n - 1) * delta_bytes : 0;
    if (src_len < DECIMAL_TOTAL_HDR_BYTES + deltas_bytes) return STRIQ_ERR_FORMAT;

    int64_t  base;
    memcpy(&base, p, 8); p += 8;

    double inv_scale = 1.0 / POW10[d_exp];

    int64_t q = base;
    out[0] = (double)q * inv_scale;

    for (size_t i = 1; i < n; i++) {
        uint64_t zz;
        if (delta_bytes == 1) {
            zz = (uint64_t)*p++;
        } else if (delta_bytes == 2) {
            zz = (uint64_t)p[0] | ((uint64_t)p[1] << 8);
            p += 2;
        } else {
            zz = (uint64_t)p[0] | ((uint64_t)p[1] << 8) |
                 ((uint64_t)p[2] << 16) | ((uint64_t)p[3] << 24);
            p += 4;
        }
        q    += zz_dec(zz);
        out[i] = (double)q * inv_scale;
    }
    return STRIQ_OK;
}
