#include "raw_stats.h"
#include <string.h>

size_t raw_stats_encode(
    const double *data,
    uint32_t      count,
    uint8_t      *out,
    size_t        out_capacity)
{
    if (!data || !out || count == 0) return 0;
    size_t needed = RAW_STATS_HDR_SIZE + (size_t)count * sizeof(double);
    if (out_capacity < needed) return 0;

    double   sum = 0.0, min_v = 0.0, max_v = 0.0, sum_sq = 0.0;
    uint64_t nz  = 0;
    striq_stats_reduce(data, count, &sum, &min_v, &max_v, &sum_sq, &nz);

    raw_stats_write_hdr(out, sum, min_v, max_v, sum_sq, (uint64_t)count, nz);

    memcpy(out + RAW_STATS_HDR_SIZE, data, (size_t)count * sizeof(double));

    return needed;
}

striq_status_t raw_stats_decode(
    const uint8_t *src,
    size_t         src_len,
    double        *out,
    size_t         n)
{
    if (!src || !out) return STRIQ_ERR_PARAM;
    size_t needed = RAW_STATS_HDR_SIZE + n * sizeof(double);
    if (src_len < needed) return STRIQ_ERR_FORMAT;
    memcpy(out, src + RAW_STATS_HDR_SIZE, n * sizeof(double));
    return STRIQ_OK;
}

striq_status_t raw_stats_parse_hdr(
    const uint8_t        *src,
    size_t                src_len,
    striq_raw_stats_hdr_t *out)
{
    if (!src || !out) return STRIQ_ERR_PARAM;
    if (src_len < RAW_STATS_HDR_SIZE) return STRIQ_ERR_FORMAT;

    const uint8_t *p = src;
    memcpy(&out->sum,      p, 8); p += 8;
    memcpy(&out->min,      p, 8); p += 8;
    memcpy(&out->max,      p, 8); p += 8;
    memcpy(&out->sum_sq,   p, 8); p += 8;
    memcpy(&out->count,    p, 8); p += 8;
    memcpy(&out->nz_count, p, 8);
    return STRIQ_OK;
}
