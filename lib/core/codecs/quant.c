#include "quant.h"
#include "../../platform/simd.h"
#include <string.h>

size_t quant_encode(
    const double *data,
    uint32_t      n,
    uint8_t       width_bits,
    uint8_t      *out,
    size_t        out_cap)
{
    if (!data || !out || n == 0) return 0;
    if (width_bits != 8 && width_bits != 16) return 0;

    uint32_t width_bytes = (uint32_t)width_bits / 8u;
    size_t   needed      = QUANT_TOTAL_HDR_BYTES + (size_t)n * width_bytes;
    if (out_cap < needed) return 0;

    double   sum = 0.0, min_v = data[0], max_v = data[0], sum_sq = 0.0;
    uint64_t nz  = 0;
    striq_stats_reduce(data, n, &sum, &min_v, &max_v, &sum_sq, &nz);

    raw_stats_write_hdr(out, sum, min_v, max_v, sum_sq, (uint64_t)n, nz);

    uint8_t *p = out + RAW_STATS_HDR_SIZE;
    memcpy(p, &min_v, 8); p += 8;
    memcpy(p, &max_v, 8); p += 8;

    double range = max_v - min_v;
    double qmax  = (width_bits == 8) ? 255.0 : 65535.0;
    double scale = (range > 1e-300) ? qmax / range : 0.0;

    if (width_bits == 8) {
        for (uint32_t i = 0; i < n; i++) {
            double q = (data[i] - min_v) * scale + 0.5;
            if (q < 0.0)   q = 0.0;
            if (q > 255.0) q = 255.0;
            *p++ = (uint8_t)q;
        }
    } else {
        for (uint32_t i = 0; i < n; i++) {
            double q = (data[i] - min_v) * scale + 0.5;
            if (q < 0.0)     q = 0.0;
            if (q > 65535.0) q = 65535.0;
            uint16_t qi = (uint16_t)q;
            p[0] = (uint8_t)(qi & 0xFF);
            p[1] = (uint8_t)(qi >> 8);
            p += 2;
        }
    }

    return needed;
}

striq_status_t quant_decode(
    const uint8_t *src,
    size_t         src_len,
    uint8_t        width_bits,
    double        *out,
    size_t         n)
{
    if (!src || !out) return STRIQ_ERR_PARAM;
    if (n == 0) return STRIQ_OK;
    if (width_bits != 8 && width_bits != 16) return STRIQ_ERR_PARAM;

    uint32_t width_bytes = (uint32_t)width_bits / 8u;
    size_t   needed      = QUANT_TOTAL_HDR_BYTES + (size_t)n * width_bytes;
    if (src_len < needed) return STRIQ_ERR_FORMAT;

    const uint8_t *p = src + RAW_STATS_HDR_SIZE;
    double min_v, max_v;
    memcpy(&min_v, p, 8); p += 8;
    memcpy(&max_v, p, 8); p += 8;

    double range = max_v - min_v;
    double qmax  = (width_bits == 8) ? 255.0 : 65535.0;
    double inv_q = (qmax > 0.0) ? range / qmax : 0.0;

    if (width_bits == 8) {
        for (size_t i = 0; i < n; i++)
            out[i] = min_v + (double)*p++ * inv_q;
    } else {
        for (size_t i = 0; i < n; i++) {
            uint16_t qi = (uint16_t)p[0] | ((uint16_t)p[1] << 8);
            p += 2;
            out[i] = min_v + (double)qi * inv_q;
        }
    }
    return STRIQ_OK;
}
