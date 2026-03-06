#include "rle_codec.h"
#include "varint.h"
#include <string.h>
#include <float.h>

#define varint_write varint_write_u32
#define varint_read  varint_read_u32

striq_status_t rle_encode(
    const double *values,
    size_t        n,
    uint8_t      *dst,
    size_t        dst_cap,
    size_t       *out_len)
{
    if (!values || !dst || !out_len || n == 0) return STRIQ_ERR_PARAM;

    double table[RLE_MAX_UNIQUE];
    uint8_t num_unique = 0;

    for (size_t i = 0; i < n; i++) {
        double v = values[i];
        int found = 0;
        for (int j = 0; j < num_unique; j++) {
            if (table[j] == v) { found = 1; break; }
        }
        if (!found) {
            if (num_unique >= RLE_MAX_UNIQUE) return STRIQ_ERR_CODEC;
            table[num_unique++] = v;
        }
    }

    /* Minimum structural overhead: 1 + table + 4-byte run count header */
    size_t hdr = 1 + (size_t)num_unique * 8 + 4;
    if (dst_cap < hdr + 12) return STRIQ_ERR_CODEC; /* need at least room for 1 run */

    uint8_t *p = dst;

    *p++ = num_unique;

    memcpy(p, table, (size_t)num_unique * 8);
    p += (size_t)num_unique * 8;

    uint8_t *num_runs_ptr = p;
    p += 4; 

    uint32_t num_runs = 0;
    size_t i = 0;
    while (i < n) {
        double v = values[i];
        uint8_t idx = 0;
        for (int j = 0; j < num_unique; j++) {
            if (table[j] == v) { idx = (uint8_t)j; break; }
        }
        size_t run_len = 1;
        while (i + run_len < n && values[i + run_len] == v) run_len++;
        if ((size_t)(p - dst) + 10 > dst_cap) return STRIQ_ERR_CODEC;

        p += varint_write(p, (uint32_t)idx);
        p += varint_write(p, (uint32_t)run_len);
        num_runs++;
        i += run_len;
    }

    num_runs_ptr[0] = (uint8_t)(num_runs & 0xFF);
    num_runs_ptr[1] = (uint8_t)((num_runs >> 8) & 0xFF);
    num_runs_ptr[2] = (uint8_t)((num_runs >> 16) & 0xFF);
    num_runs_ptr[3] = (uint8_t)((num_runs >> 24) & 0xFF);

    *out_len = (size_t)(p - dst);
    return STRIQ_OK;
}

striq_status_t rle_decode(
    const uint8_t *src,
    size_t         src_len,
    double        *values,
    size_t         n)
{
    if (!src || !values || src_len < 1) return STRIQ_ERR_PARAM;

    size_t pos = 0;
    uint8_t num_unique = src[pos++];
    if (num_unique == 0 || num_unique > RLE_MAX_UNIQUE) return STRIQ_ERR_FORMAT;

    if (pos + (size_t)num_unique * 8 > src_len) return STRIQ_ERR_FORMAT;
    double table[RLE_MAX_UNIQUE];
    memcpy(table, src + pos, (size_t)num_unique * 8);
    pos += (size_t)num_unique * 8;

    if (pos + 4 > src_len) return STRIQ_ERR_FORMAT;
    uint32_t num_runs = (uint32_t)src[pos]
        | ((uint32_t)src[pos+1] << 8)
        | ((uint32_t)src[pos+2] << 16)
        | ((uint32_t)src[pos+3] << 24);
    pos += 4;

    size_t out_pos = 0;
    for (uint32_t r = 0; r < num_runs; r++) {
        uint32_t idx = 0, run_len = 0;
        size_t nb = varint_read(src, src_len, pos, &idx);
        if (nb == 0) return STRIQ_ERR_FORMAT;
        pos += nb;
        nb = varint_read(src, src_len, pos, &run_len);
        if (nb == 0) return STRIQ_ERR_FORMAT;
        pos += nb;

        if (idx >= num_unique) return STRIQ_ERR_FORMAT;
        double v = table[idx];

        if (out_pos + run_len > n) run_len = (uint32_t)(n - out_pos);
        for (uint32_t k = 0; k < run_len; k++)
            values[out_pos++] = v;
    }

    return STRIQ_OK;
}

static striq_status_t rle_parse(
    const uint8_t *src, size_t src_len,
    double        *table,      /* caller must provide RLE_MAX_UNIQUE slots */
    uint8_t       *p_num_unique,
    uint32_t      *run_idx,    /* caller-alloc, large enough */
    uint32_t      *run_len,
    uint32_t      *p_num_runs)
{
    if (!src || src_len < 1) return STRIQ_ERR_PARAM;
    size_t pos = 0;

    uint8_t nu = src[pos++];
    if (nu == 0 || nu > RLE_MAX_UNIQUE) return STRIQ_ERR_FORMAT;
    *p_num_unique = nu;

    if (pos + (size_t)nu * 8 > src_len) return STRIQ_ERR_FORMAT;
    memcpy(table, src + pos, (size_t)nu * 8);
    pos += (size_t)nu * 8;

    if (pos + 4 > src_len) return STRIQ_ERR_FORMAT;
    uint32_t nr = (uint32_t)src[pos]
        | ((uint32_t)src[pos+1] << 8)
        | ((uint32_t)src[pos+2] << 16)
        | ((uint32_t)src[pos+3] << 24);
    pos += 4;
    *p_num_runs = nr;

    for (uint32_t r = 0; r < nr; r++) {
        uint32_t idx = 0, len = 0;
        size_t nb = varint_read(src, src_len, pos, &idx);
        if (nb == 0) return STRIQ_ERR_FORMAT;
        pos += nb;
        nb = varint_read(src, src_len, pos, &len);
        if (nb == 0) return STRIQ_ERR_FORMAT;
        pos += nb;
        run_idx[r] = idx;
        run_len[r] = len;
    }
    return STRIQ_OK;
}

striq_status_t rle_query_stats(
    const uint8_t *src,
    size_t         src_len,
    rle_stats_t   *out)
{
    if (!out) return STRIQ_ERR_PARAM;

    double table[RLE_MAX_UNIQUE];
    uint8_t  nu;
    uint32_t run_idx[65536], run_len[65536], nr;

    striq_status_t s = rle_parse(src, src_len, table, &nu, run_idx, run_len, &nr);
    if (s != STRIQ_OK) return s;

    double sum = 0.0, sum2 = 0.0;
    double mn = DBL_MAX, mx = -DBL_MAX;
    uint64_t total = 0;

    for (uint32_t r = 0; r < nr; r++) {
        double v = table[run_idx[r]];
        uint64_t L = run_len[r];
        sum  += v * (double)L;
        sum2 += v * v * (double)L;
        total += L;
        if (v < mn) mn = v;
        if (v > mx) mx = v;
    }

    out->count    = total;
    out->sum      = sum;
    out->mean     = total > 0 ? sum / (double)total : 0.0;
    out->min      = mn < DBL_MAX ? mn : 0.0;
    out->max      = mx > -DBL_MAX ? mx : 0.0;
    out->variance = total > 1
        ? (sum2 / (double)total - out->mean * out->mean)
        : 0.0;
    return STRIQ_OK;
}

striq_status_t rle_query_count_where(
    const uint8_t *src,
    size_t         src_len,
    double         threshold,
    striq_cmp_t    cmp,
    uint64_t      *out_count)
{
    if (!out_count) return STRIQ_ERR_PARAM;

    double table[RLE_MAX_UNIQUE];
    uint8_t  nu;
    uint32_t run_idx[65536], run_len[65536], nr;

    striq_status_t s = rle_parse(src, src_len, table, &nu, run_idx, run_len, &nr);
    if (s != STRIQ_OK) return s;

    uint64_t cnt = 0;
    for (uint32_t r = 0; r < nr; r++) {
        double v = table[run_idx[r]];
        int match = 0;
        switch (cmp) {
            case STRIQ_CMP_GT:  match = v >  threshold; break;
            case STRIQ_CMP_GTE: match = v >= threshold; break;
            case STRIQ_CMP_LT:  match = v <  threshold; break;
            case STRIQ_CMP_LTE: match = v <= threshold; break;
            case STRIQ_CMP_EQ:  match = v == threshold; break;
        }
        if (match) cnt += run_len[r];
    }
    *out_count = cnt;
    return STRIQ_OK;
}
