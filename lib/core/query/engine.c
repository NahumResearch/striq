#include "engine.h"
#include "algebra.h"
#include "../format/endian.h"
#include "../codecs/pla.h"
#include "../codecs/chebyshev.h"
#include "../codecs/dod.h"
#include "../codecs/raw_stats.h"
#include "../codecs/rle_codec.h"
#include "../codecs/decimal.h"
#include "../codecs/quant.h"
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <math.h>

/*
 * Kahan compensated summation — prevents floating-point drift when
 * accumulating thousands of partial sums from different blocks.
 */
static inline void kahan_add(double *sum, double *comp, double val)
{
    double y = val - *comp;
    double t = *sum + y;
    *comp     = (t - *sum) - y;
    *sum      = t;
}

striq_status_t engine_init(
    striq_query_engine_t   *e,
    striq_block_provider_t *provider)
{
    if (!e || !provider) return STRIQ_ERR_PARAM;
    e->provider = provider;
    return STRIQ_OK;
}

#define rd_u32e read_u32_le

static int find_col(striq_block_provider_t *p, const char *name)
{
    for (uint32_t c = 0; c < p->num_cols; c++) {
        if (strncmp(p->col_schemas[c].name, name, 63) == 0) return (int)c;
    }
    return -1;
}

static bool block_in_range(
    const striq_block_index_t *bi,
    int64_t ts_from, int64_t ts_to)
{
    if (ts_from == 0 && ts_to == 0) return true;
    if (ts_to   != 0 && bi->ts_first > ts_to)  return false;
    if (ts_from != 0 && bi->ts_last  < ts_from) return false;
    return true;
}

typedef struct {
    const uint8_t *buf;
    size_t         blen;
    uint32_t       block_idx;
    striq_block_provider_t *provider;

    uint16_t  idx_count;
    striq_ts_index_entry_t idx[STRIQ_TS_INDEX_MAX_ENTRIES];
    const uint8_t *dod_stream;
    uint32_t       dod_len;

    size_t    col_start;
    uint32_t  num_rows;
} block_ctx_t;

static void ctx_release(block_ctx_t *ctx)
{
    if (ctx->buf && ctx->provider)
        ctx->provider->release_block(ctx->provider->ctx,
                                     ctx->block_idx, ctx->buf);
    ctx->buf = NULL;
}

static striq_status_t block_ctx_open(
    striq_query_engine_t *e,
    uint32_t              block_idx,
    block_ctx_t          *ctx)
{
    striq_block_provider_t *p = e->provider;

    striq_block_index_t bi;
    STRIQ_TRY(p->get_block_index(p->ctx, block_idx, &bi));

    uint32_t bsz = 0;
    striq_status_t s = p->get_block_data(p->ctx, block_idx, &ctx->buf, &bsz);
    if (s != STRIQ_OK) { ctx->buf = NULL; return s; }

    ctx->blen      = bsz;
    ctx->block_idx = block_idx;
    ctx->provider  = p;
    ctx->num_rows  = bi.num_rows;

    size_t pos = 24;
    if (pos + 4 > ctx->blen) { ctx_release(ctx); return STRIQ_ERR_FORMAT; }

    uint32_t ts_outer = rd_u32e(ctx->buf + pos); pos += 4;
    size_t ts_section_start = pos;

    ctx->idx_count = (uint16_t)ctx->buf[pos] | ((uint16_t)ctx->buf[pos+1] << 8);
    pos += 2;

    uint16_t nc = ctx->idx_count;
    if (nc > STRIQ_TS_INDEX_MAX_ENTRIES) nc = STRIQ_TS_INDEX_MAX_ENTRIES;
    for (uint16_t ix = 0; ix < nc; ix++) {
        int64_t ts_val = 0;
        for (int b = 0; b < 8; b++)
            ts_val |= (int64_t)ctx->buf[pos + b] << (b * 8);
        pos += 8;
        ctx->idx[ix].ts          = ts_val;
        ctx->idx[ix].row_offset  = rd_u32e(ctx->buf + pos); pos += 4;
        ctx->idx[ix].byte_offset = rd_u32e(ctx->buf + pos); pos += 4;
    }
    if (ctx->idx_count > STRIQ_TS_INDEX_MAX_ENTRIES)
        pos += (size_t)(ctx->idx_count - STRIQ_TS_INDEX_MAX_ENTRIES) * 16;

    ctx->dod_len    = rd_u32e(ctx->buf + pos); pos += 4;
    ctx->dod_stream = ctx->buf + pos;

    ctx->col_start = ts_section_start + ts_outer;
    return STRIQ_OK;
}

/*
 * Read column metadata from an open block.
 * For PLA_LINEAR: returns seg_buf (18-byte stride), seg_count.
 * For PLA_CHEB:   returns seg_buf (34-byte stride), seg_count.
 * For RLE/RAW_STATS: returns raw_data, raw_len, and codec_byte.
 *                    Returns STRIQ_ERR_QUERY to distinguish from errors.
 */
static striq_status_t ctx_read_col_base(
    const block_ctx_t *ctx,
    uint32_t           col_idx,
    const uint8_t    **out_seg_buf,
    size_t            *out_seg_count,
    uint8_t           *out_codec_byte,
    const uint8_t    **out_raw_data,
    size_t            *out_raw_len)
{
    size_t pos = ctx->col_start;

    for (uint32_t c = 0; c <= col_idx; c++) {
        if (pos >= ctx->blen) return STRIQ_ERR_FORMAT;

        uint8_t codec_byte = ctx->buf[pos++];
        striq_codec_t base = CODEC_BASE(codec_byte);
        uint32_t base_len  = rd_u32e(ctx->buf + pos); pos += 4;

        if (c == col_idx) {
            if (out_codec_byte) *out_codec_byte = codec_byte;

            if (base == CODEC_PLA_LINEAR) {
                if (out_seg_buf)   *out_seg_buf   = ctx->buf + pos;
                if (out_seg_count) *out_seg_count = base_len / 18u;
                return STRIQ_OK;
            }
            if (base == CODEC_PLA_CHEB) {
                if (out_seg_buf)   *out_seg_buf   = ctx->buf + pos;
                if (out_seg_count) *out_seg_count = base_len / CHEB_SEG_BYTES;
                return STRIQ_OK;
            }
            if (out_raw_data) *out_raw_data = ctx->buf + pos;
            if (out_raw_len)  *out_raw_len  = base_len;
            return STRIQ_ERR_QUERY;
        }

        pos += base_len;
        uint32_t resid_len = rd_u32e(ctx->buf + pos); pos += 4 + resid_len;
    }
    return STRIQ_ERR_NOTFOUND;
}

/*
 * get_block_values — extract raw doubles from a non-PLA block.
 *
 * Handles RAW_STATS (direct copy), DECIMAL (lossless reconstruct),
 * QUANT8/16 (lossy linear rescale), and RLE (run-length decode).
 */
static striq_status_t get_block_values(
    const uint8_t *block_data, size_t block_len,
    uint8_t        codec_byte,
    uint32_t       n_rows,
    double       **out_values)
{
    striq_codec_t base = CODEC_BASE(codec_byte);

    double *buf = malloc((size_t)n_rows * sizeof(double));
    if (!buf) return STRIQ_ERR_MEMORY;

    striq_status_t s = STRIQ_ERR_FORMAT;
    if (base == CODEC_RAW_STATS) {
        size_t needed = RAW_STATS_HDR_SIZE + (size_t)n_rows * sizeof(double);
        if (block_len >= needed) {
            memcpy(buf, block_data + RAW_STATS_HDR_SIZE,
                   (size_t)n_rows * sizeof(double));
            s = STRIQ_OK;
        }
    } else if (base == CODEC_DECIMAL) {
        s = decimal_decode(block_data, block_len, buf, n_rows);
    } else if (base == CODEC_QUANT16) {
        s = quant_decode(block_data, block_len, 16, buf, n_rows);
    } else if (base == CODEC_QUANT8) {
        s = quant_decode(block_data, block_len, 8, buf, n_rows);
    } else if (base == CODEC_RLE) {
        s = rle_decode(block_data, block_len, buf, n_rows);
    }

    if (s != STRIQ_OK) { free(buf); return s; }
    *out_values = buf;
    return STRIQ_OK;
}

/*
 * Pre-computed per-block per-column stats are loaded from the file footer
 * into RAM at open time.  For whole-file aggregate queries (no timestamp
 * filter), these let us bypass block I/O entirely.
 */
static striq_status_t fast_col_mean(
    striq_query_engine_t *e,
    int ci,
    double *out_sum, uint64_t *out_n)
{
    striq_block_provider_t *p = e->provider;
    if (!p->get_col_stats) return STRIQ_ERR_NOTFOUND;
    double total_sum = 0.0, kcomp = 0.0; uint64_t total_n = 0;
    for (uint32_t bi = 0; bi < p->num_blocks; bi++) {
        striq_col_stats_t st;
        if (p->get_col_stats(p->ctx, bi, (uint32_t)ci, &st) != STRIQ_OK) continue;
        kahan_add(&total_sum, &kcomp, st.sum);
        total_n += st.count;
    }
    if (total_n == 0) return STRIQ_ERR_NOTFOUND;
    *out_sum = total_sum;
    *out_n   = total_n;
    return STRIQ_OK;
}

static striq_status_t fast_col_minmax(
    striq_query_engine_t *e,
    int ci, bool want_min, double *out_val)
{
    striq_block_provider_t *p = e->provider;
    if (!p->get_col_stats) return STRIQ_ERR_NOTFOUND;
    double result = 0.0; bool found = false;
    for (uint32_t bi = 0; bi < p->num_blocks; bi++) {
        striq_col_stats_t st;
        if (p->get_col_stats(p->ctx, bi, (uint32_t)ci, &st) != STRIQ_OK) continue;
        double v = want_min ? st.min : st.max;
        if (!found || (want_min ? v < result : v > result)) {
            result = v; found = true;
        }
    }
    if (!found) return STRIQ_ERR_NOTFOUND;
    *out_val = result;
    return STRIQ_OK;
}

static void refine_row_range(
    const block_ctx_t *ctx,
    int64_t ts_from, int64_t ts_to,
    uint32_t *start_row, uint32_t *end_row)
{
    if (ts_from == 0 && ts_to == 0) return;
    uint32_t scan_n = *end_row - *start_row + 1;
    if (scan_n == 0) return;

    int64_t *scan_ts = malloc(scan_n * sizeof(int64_t));
    if (!scan_ts) return;

    if (dod_decode_range(ctx->dod_stream, ctx->dod_len, ctx->num_rows,
                         ctx->idx, ctx->idx_count,
                         *start_row, *end_row, scan_ts) == STRIQ_OK) {
        if (ts_from != 0) {
            for (uint32_t r = 0; r < scan_n; r++) {
                if (scan_ts[r] >= ts_from) { *start_row += r; break; }
            }
        }
        if (ts_to != 0) {
            uint32_t new_end = *start_row;
            for (uint32_t r = 0; r < scan_n; r++) {
                if (scan_ts[r] <= ts_to) new_end = *start_row + r;
            }
            *end_row = new_end;
        }
    }
    free(scan_ts);
}

static void sum_linear_segs(
    const uint8_t *seg_buf, size_t seg_count,
    uint32_t start_row, uint32_t end_row,
    double *total_sum, double *kcomp, uint64_t *total_n, uint64_t *algebraic_n)
{
    uint32_t row_cursor = 0;
    for (size_t s = 0; s < seg_count; s++) {
        double slope, offset; uint16_t L;
        const uint8_t *sp = seg_buf + s * 18u;
        memcpy(&slope,  sp,      8);
        memcpy(&offset, sp + 8,  8);
        memcpy(&L,      sp + 16, 2);

        uint32_t seg_start = row_cursor;
        uint32_t seg_end   = row_cursor + (uint32_t)L - 1;
        row_cursor += (uint32_t)L;

        if (seg_end   < start_row) continue;
        if (seg_start > end_row)   break;

        uint32_t clip_s = (seg_start >= start_row) ? 0
                        : (start_row - seg_start);
        uint32_t clip_e = (uint32_t)L - 1;
        if (seg_end > end_row) clip_e = end_row - seg_start;

        double psum = algebra_partial_sum(slope, offset, clip_s, clip_e);
        uint32_t cnt = clip_e - clip_s + 1;
        kahan_add(total_sum, kcomp, psum);
        *total_n     += cnt;
        *algebraic_n += cnt;
    }
}

static void sum_cheb_segs(
    const uint8_t *seg_buf, size_t seg_count,
    uint32_t start_row, uint32_t end_row,
    double *total_sum, double *kcomp, uint64_t *total_n, uint64_t *algebraic_n)
{
    uint32_t row_cursor = 0;
    for (size_t s = 0; s < seg_count; s++) {
        double c[4]; uint16_t L;
        cheb_seg_read(seg_buf + s * CHEB_SEG_BYTES, c, &L);

        uint32_t seg_start = row_cursor;
        uint32_t seg_end   = row_cursor + (uint32_t)L - 1;
        row_cursor += (uint32_t)L;

        if (seg_end   < start_row) continue;
        if (seg_start > end_row)   break;

        uint32_t clip_s = (seg_start >= start_row) ? 0
                        : (start_row - seg_start);
        uint32_t clip_e = (uint32_t)L - 1;
        if (seg_end > end_row) clip_e = end_row - seg_start;

        double psum = algebra_cheb_partial_sum(c, L, clip_s, clip_e);
        uint32_t cnt = clip_e - clip_s + 1;
        kahan_add(total_sum, kcomp, psum);
        *total_n     += cnt;
        *algebraic_n += cnt;
    }
}

striq_status_t engine_query_mean(
    striq_query_engine_t *e,
    const char           *col_name,
    int64_t               ts_from,
    int64_t               ts_to,
    striq_query_result_t *out)
{
    if (!e || !col_name || !out) return STRIQ_ERR_PARAM;
    striq_block_provider_t *p = e->provider;

    int ci = find_col(p, col_name);
    if (ci < 0) return STRIQ_ERR_NOTFOUND;

    bool no_filter = (ts_from == 0 && ts_to == 0);
    if (no_filter) {
        double fast_sum = 0.0; uint64_t fast_n = 0;
        if (fast_col_mean(e, ci, &fast_sum, &fast_n) == STRIQ_OK) {
            out->value         = fast_sum / (double)fast_n;
            out->error_bound   = p->col_epsilons[(uint32_t)ci];
            out->rows_scanned  = fast_n;
            out->pct_data_read = 0.0;
            out->pct_algebraic = 100.0;
            return STRIQ_OK;
        }
    }

    double   total_sum   = 0.0, kcomp = 0.0;
    uint64_t total_n     = 0;
    uint64_t algebraic_n = 0;

    for (uint32_t bi = 0; bi < p->num_blocks; bi++) {
        striq_block_index_t bidx;
        if (p->get_block_index(p->ctx, bi, &bidx) != STRIQ_OK) continue;
        if (!block_in_range(&bidx, ts_from, ts_to)) continue;

        block_ctx_t ctx;
        striq_status_t rs = block_ctx_open(e, bi, &ctx);
        if (rs != STRIQ_OK) return rs;

        const uint8_t *seg_buf   = NULL;
        size_t         seg_count = 0;
        const uint8_t *raw_data  = NULL;
        size_t         raw_len   = 0;
        uint8_t        codec_byte = 0;
        rs = ctx_read_col_base(&ctx, (uint32_t)ci, &seg_buf, &seg_count,
                               &codec_byte, &raw_data, &raw_len);

        if (rs == STRIQ_ERR_QUERY) {
            striq_codec_t non_pla = CODEC_BASE(codec_byte);
            if (non_pla == CODEC_RLE) {
                rle_stats_t rle_st;
                if (rle_query_stats(raw_data, raw_len, &rle_st) == STRIQ_OK) {
                    if (no_filter) {
                        kahan_add(&total_sum, &kcomp, rle_st.sum);
                        total_n     += rle_st.count;
                        algebraic_n += rle_st.count;
                    } else {
                        double *vals = NULL;
                        if (get_block_values(raw_data, raw_len, codec_byte,
                                             ctx.num_rows, &vals) == STRIQ_OK && vals) {
                            uint32_t r0 = 0, r1 = ctx.num_rows - 1;
                            dod_find_range(ctx.idx, ctx.idx_count, ctx.num_rows,
                                           ts_from, ts_to, &r0, &r1);
                            refine_row_range(&ctx, ts_from, ts_to, &r0, &r1);
                            for (uint32_t r = r0; r <= r1; r++) {
                                kahan_add(&total_sum, &kcomp, vals[r]); total_n++;
                            }
                            free(vals);
                        }
                    }
                }
            } else if (non_pla == CODEC_RAW_STATS ||
                       non_pla == CODEC_DECIMAL    ||
                       non_pla == CODEC_QUANT8     ||
                       non_pla == CODEC_QUANT16) {
                if (no_filter) {
                    striq_raw_stats_hdr_t hdr;
                    if (raw_stats_parse_hdr(raw_data, raw_len, &hdr) == STRIQ_OK) {
                        kahan_add(&total_sum, &kcomp, hdr.sum);
                        total_n     += hdr.count;
                        algebraic_n += hdr.count;
                    }
                } else {
                    double *vals = NULL;
                    if (get_block_values(raw_data, raw_len, codec_byte,
                                         ctx.num_rows, &vals) == STRIQ_OK) {
                        uint32_t r0 = 0, r1 = ctx.num_rows - 1;
                        dod_find_range(ctx.idx, ctx.idx_count, ctx.num_rows,
                                       ts_from, ts_to, &r0, &r1);
                        refine_row_range(&ctx, ts_from, ts_to, &r0, &r1);
                        for (uint32_t r = r0; r <= r1; r++) {
                            kahan_add(&total_sum, &kcomp, vals[r]); total_n++;
                        }
                        free(vals);
                    }
                }
            }
            ctx_release(&ctx);
            continue;
        }
        if (rs != STRIQ_OK) { ctx_release(&ctx); return rs; }

        striq_codec_t base_codec = CODEC_BASE(codec_byte);

        if (no_filter) {
            if (base_codec == CODEC_PLA_CHEB) {
                uint64_t blk_n = 0;
                double   blk_mean = 0.0;
                if (algebra_cheb_mean(seg_buf, seg_count, &blk_mean, &blk_n)
                    == STRIQ_OK) {
                    kahan_add(&total_sum, &kcomp, blk_mean * (double)blk_n);
                    total_n     += blk_n;
                    algebraic_n += blk_n;
                }
            } else {
                double blk_mean = 0.0; uint64_t blk_n = 0; double blk_err = 0.0;
                if (algebra_mean(seg_buf, seg_count, p->col_epsilons[(uint32_t)ci],
                                 &blk_mean, &blk_n, &blk_err) == STRIQ_OK) {
                    kahan_add(&total_sum, &kcomp, blk_mean * (double)blk_n);
                    total_n     += blk_n;
                    algebraic_n += blk_n;
                }
            }
        } else {
            uint32_t start_row = 0, end_row = ctx.num_rows - 1;
            dod_find_range(ctx.idx, ctx.idx_count, ctx.num_rows,
                           ts_from, ts_to, &start_row, &end_row);
            refine_row_range(&ctx, ts_from, ts_to, &start_row, &end_row);

            if (base_codec == CODEC_PLA_CHEB) {
                sum_cheb_segs(seg_buf, seg_count, start_row, end_row,
                              &total_sum, &kcomp, &total_n, &algebraic_n);
            } else {
                sum_linear_segs(seg_buf, seg_count, start_row, end_row,
                                &total_sum, &kcomp, &total_n, &algebraic_n);
            }
        }
        ctx_release(&ctx);
    }

    if (total_n == 0) return STRIQ_ERR_NOTFOUND;

    out->value         = total_sum / (double)total_n;
    out->error_bound   = p->col_epsilons[(uint32_t)ci];
    out->rows_scanned  = total_n;
    out->pct_data_read = 100.0 * (double)total_n / (double)p->total_rows;
    out->pct_algebraic = (total_n > 0)
                       ? 100.0 * (double)algebraic_n / (double)total_n
                       : 0.0;
    return STRIQ_OK;
}

striq_status_t engine_query_count(
    striq_query_engine_t *e,
    int64_t               ts_from,
    int64_t               ts_to,
    uint64_t             *out)
{
    if (!e || !out) return STRIQ_ERR_PARAM;
    striq_block_provider_t *p = e->provider;
    bool no_filter = (ts_from == 0 && ts_to == 0);
    uint64_t total = 0;

    for (uint32_t bi = 0; bi < p->num_blocks; bi++) {
        striq_block_index_t bidx;
        if (p->get_block_index(p->ctx, bi, &bidx) != STRIQ_OK) continue;
        if (!block_in_range(&bidx, ts_from, ts_to)) continue;

        if (no_filter) { total += bidx.num_rows; continue; }

        block_ctx_t ctx;
        striq_status_t rs = block_ctx_open(e, bi, &ctx);
        if (rs != STRIQ_OK) return rs;

        uint32_t start_row = 0, end_row = ctx.num_rows - 1;
        dod_find_range(ctx.idx, ctx.idx_count, ctx.num_rows,
                       ts_from, ts_to, &start_row, &end_row);
        refine_row_range(&ctx, ts_from, ts_to, &start_row, &end_row);
        ctx_release(&ctx);

        total += end_row - start_row + 1;
    }
    *out = total;
    return STRIQ_OK;
}

striq_status_t engine_query_mean_where(
    striq_query_engine_t *e,
    const char           *col_name,
    int64_t               ts_from,
    int64_t               ts_to,
    double                threshold,
    striq_cmp_t           cmp,
    striq_query_result_t *out)
{
    if (!e || !col_name || !out) return STRIQ_ERR_PARAM;
    striq_block_provider_t *p = e->provider;

    int ci = find_col(p, col_name);
    if (ci < 0) return STRIQ_ERR_NOTFOUND;

    double   total_sum = 0.0;
    uint64_t total_n   = 0;

    for (uint32_t bi = 0; bi < p->num_blocks; bi++) {
        striq_block_index_t bidx;
        if (p->get_block_index(p->ctx, bi, &bidx) != STRIQ_OK) continue;
        if (!block_in_range(&bidx, ts_from, ts_to)) continue;

        block_ctx_t ctx;
        striq_status_t rs = block_ctx_open(e, bi, &ctx);
        if (rs != STRIQ_OK) return rs;

        const uint8_t *seg_buf   = NULL;
        size_t         seg_count = 0;
        const uint8_t *raw_data  = NULL;
        size_t         raw_len   = 0;
        uint8_t        codec_byte = 0;
        rs = ctx_read_col_base(&ctx, (uint32_t)ci, &seg_buf, &seg_count,
                               &codec_byte, &raw_data, &raw_len);

        if (rs == STRIQ_ERR_QUERY) {
            striq_codec_t non_pla3 = CODEC_BASE(codec_byte);
            double *vals = NULL;
            get_block_values(raw_data, raw_len, codec_byte, ctx.num_rows, &vals);
            (void)non_pla3;
            if (vals) {
                uint32_t r0 = 0, r1 = ctx.num_rows - 1;
                if (ts_from != 0 || ts_to != 0) {
                    dod_find_range(ctx.idx, ctx.idx_count, ctx.num_rows,
                                   ts_from, ts_to, &r0, &r1);
                    refine_row_range(&ctx, ts_from, ts_to, &r0, &r1);
                }
                for (uint32_t r = r0; r <= r1; r++) {
                    bool pass = false;
                    switch (cmp) {
                        case STRIQ_CMP_LT:  pass = vals[r] <  threshold; break;
                        case STRIQ_CMP_LTE: pass = vals[r] <= threshold; break;
                        case STRIQ_CMP_EQ:  pass = vals[r] == threshold; break;
                        case STRIQ_CMP_GTE: pass = vals[r] >= threshold; break;
                        case STRIQ_CMP_GT:  pass = vals[r] >  threshold; break;
                        default: break;
                    }
                    if (pass) { total_sum += vals[r]; total_n++; }
                }
                free(vals);
            }
            ctx_release(&ctx);
            continue;
        }
        if (rs != STRIQ_OK) { ctx_release(&ctx); return rs; }

        striq_codec_t base_codec = CODEC_BASE(codec_byte);

        uint32_t start_row = 0, end_row = ctx.num_rows - 1;
        if (ts_from != 0 || ts_to != 0) {
            dod_find_range(ctx.idx, ctx.idx_count, ctx.num_rows,
                           ts_from, ts_to, &start_row, &end_row);
            refine_row_range(&ctx, ts_from, ts_to, &start_row, &end_row);
        }

        uint32_t row_cursor = 0;

        if (base_codec == CODEC_PLA_CHEB) {
            /* For Chebyshev where-predicates: evaluate at each point */
            for (size_t s = 0; s < seg_count; s++) {
                double c[4]; uint16_t L;
                cheb_seg_read(seg_buf + s * CHEB_SEG_BYTES, c, &L);

                uint32_t seg_start = row_cursor;
                uint32_t seg_end   = row_cursor + (uint32_t)L - 1;
                row_cursor += (uint32_t)L;

                if (seg_end   < start_row) continue;
                if (seg_start > end_row)   break;

                uint32_t clip_s = (seg_start >= start_row) ? 0
                                : (start_row - seg_start);
                uint32_t clip_e = (uint32_t)L - 1;
                if (seg_end > end_row) clip_e = end_row - seg_start;

                double inv = (L > 1) ? 2.0 / (double)(L - 1) : 0.0;
                for (uint32_t j = clip_s; j <= clip_e; j++) {
                    double u = (L > 1) ? inv * (double)j - 1.0 : 0.0;
                    double v = cheb3_eval(c, u);
                    bool pass = false;
                    switch (cmp) {
                        case STRIQ_CMP_LT:  pass = v <  threshold; break;
                        case STRIQ_CMP_LTE: pass = v <= threshold; break;
                        case STRIQ_CMP_EQ:  pass = v == threshold; break;
                        case STRIQ_CMP_GTE: pass = v >= threshold; break;
                        case STRIQ_CMP_GT:  pass = v >  threshold; break;
                        default: break;
                    }
                    if (pass) { total_sum += v; total_n++; }
                }
            }
        } else {
            for (size_t s = 0; s < seg_count; s++) {
                double slope, offset; uint16_t L;
                const uint8_t *sp = seg_buf + s * 18u;
                memcpy(&slope,  sp,      8);
                memcpy(&offset, sp + 8,  8);
                memcpy(&L,      sp + 16, 2);

                uint32_t seg_start = row_cursor;
                uint32_t seg_end   = row_cursor + (uint32_t)L - 1;
                row_cursor += (uint32_t)L;

                if (seg_end   < start_row) continue;
                if (seg_start > end_row)   break;

                uint32_t clip_s = (seg_start >= start_row) ? 0 : (start_row - seg_start);
                uint32_t clip_e = (uint32_t)L - 1;
                if (seg_end > end_row) clip_e = end_row - seg_start;

                double seg_sum = 0.0; uint64_t seg_cnt = 0;
                rs = algebra_sum_where(slope, offset,
                                       (uint16_t)(clip_e - clip_s + 1),
                                       threshold, cmp, &seg_sum, &seg_cnt);
                if (rs == STRIQ_OK) { total_sum += seg_sum; total_n += seg_cnt; }
            }
        }
        ctx_release(&ctx);
    }

    if (total_n == 0) return STRIQ_ERR_NOTFOUND;

    out->value         = total_sum / (double)total_n;
    out->error_bound   = p->col_epsilons[(uint32_t)ci];
    out->rows_scanned  = total_n;
    out->pct_data_read = 100.0 * (double)total_n / (double)p->total_rows;
    out->pct_algebraic = 100.0;
    return STRIQ_OK;
}

static striq_status_t engine_query_minmax(
    striq_query_engine_t *e,
    const char           *col_name,
    int64_t               ts_from,
    int64_t               ts_to,
    bool                  want_min,
    striq_query_result_t *out)
{
    if (!e || !col_name || !out) return STRIQ_ERR_PARAM;
    striq_block_provider_t *p = e->provider;

    int ci = find_col(p, col_name);
    if (ci < 0) return STRIQ_ERR_NOTFOUND;

    bool no_filter = (ts_from == 0 && ts_to == 0);
    if (no_filter) {
        double fast_val = 0.0;
        if (fast_col_minmax(e, ci, want_min, &fast_val) == STRIQ_OK) {
            out->value         = fast_val;
            out->error_bound   = p->col_epsilons[(uint32_t)ci];
            out->rows_scanned  = p->total_rows;
            out->pct_data_read = 0.0;
            out->pct_algebraic = 100.0;
            return STRIQ_OK;
        }
    }

    double global_val = 0.0;
    bool   found      = false;

    for (uint32_t bi = 0; bi < p->num_blocks; bi++) {
        striq_block_index_t bidx;
        if (p->get_block_index(p->ctx, bi, &bidx) != STRIQ_OK) continue;
        if (!block_in_range(&bidx, ts_from, ts_to)) continue;

        block_ctx_t ctx;
        striq_status_t rs = block_ctx_open(e, bi, &ctx);
        if (rs != STRIQ_OK) return rs;

        const uint8_t *seg_buf   = NULL;
        size_t         seg_count = 0;
        const uint8_t *raw_data  = NULL;
        size_t         raw_len   = 0;
        uint8_t        codec_byte = 0;
        rs = ctx_read_col_base(&ctx, (uint32_t)ci, &seg_buf, &seg_count,
                               &codec_byte, &raw_data, &raw_len);

        if (rs == STRIQ_ERR_QUERY) {
            striq_codec_t non_pla2 = CODEC_BASE(codec_byte);
            double *vals = NULL;

            if (non_pla2 == CODEC_RLE) {
                if (no_filter) {
                    rle_stats_t rle_st;
                    if (rle_query_stats(raw_data, raw_len, &rle_st) == STRIQ_OK) {
                        double v = want_min ? rle_st.min : rle_st.max;
                        if (!found || (want_min ? v < global_val : v > global_val)) {
                            global_val = v; found = true;
                        }
                    }
                    ctx_release(&ctx); continue;
                }
                get_block_values(raw_data, raw_len, codec_byte, ctx.num_rows, &vals);
            } else if (non_pla2 == CODEC_RAW_STATS ||
                       non_pla2 == CODEC_DECIMAL    ||
                       non_pla2 == CODEC_QUANT8     ||
                       non_pla2 == CODEC_QUANT16) {
                if (no_filter) {
                    striq_raw_stats_hdr_t hdr;
                    if (raw_stats_parse_hdr(raw_data, raw_len, &hdr) == STRIQ_OK) {
                        double v = want_min ? hdr.min : hdr.max;
                        if (!found || (want_min ? v < global_val : v > global_val)) {
                            global_val = v; found = true;
                        }
                    }
                    ctx_release(&ctx); continue;
                }
                get_block_values(raw_data, raw_len, codec_byte, ctx.num_rows, &vals);
            }

            if (vals) {
                uint32_t r0 = 0, r1 = ctx.num_rows - 1;
                dod_find_range(ctx.idx, ctx.idx_count, ctx.num_rows,
                               ts_from, ts_to, &r0, &r1);
                refine_row_range(&ctx, ts_from, ts_to, &r0, &r1);
                for (uint32_t r = r0; r <= r1; r++) {
                    if (!found || (want_min ? vals[r] < global_val
                                           : vals[r] > global_val)) {
                        global_val = vals[r]; found = true;
                    }
                }
                free(vals);
            }
            ctx_release(&ctx);
            continue;
        }
        if (rs != STRIQ_OK) { ctx_release(&ctx); return rs; }

        striq_codec_t base_codec = CODEC_BASE(codec_byte);

        if (no_filter) {
            if (base_codec == CODEC_PLA_CHEB) {
                double seg_min = 0, seg_max = 0;
                if (algebra_cheb_min_max(seg_buf, seg_count, p->col_epsilons[(uint32_t)ci],
                                         &seg_min, &seg_max) == STRIQ_OK) {
                    double v = want_min ? seg_min : seg_max;
                    if (!found || (want_min ? v < global_val : v > global_val)) {
                        global_val = v; found = true;
                    }
                }
            } else {
                striq_query_result_t blk_out;
                rs = want_min ? algebra_min(seg_buf, seg_count, p->col_epsilons[(uint32_t)ci], &blk_out)
                              : algebra_max(seg_buf, seg_count, p->col_epsilons[(uint32_t)ci], &blk_out);
                if (rs == STRIQ_OK) {
                    if (!found || (want_min ? blk_out.value < global_val
                                           : blk_out.value > global_val)) {
                        global_val = blk_out.value; found = true;
                    }
                }
            }
            ctx_release(&ctx);
        } else {
            uint32_t start_row = 0, end_row = ctx.num_rows - 1;
            dod_find_range(ctx.idx, ctx.idx_count, ctx.num_rows,
                           ts_from, ts_to, &start_row, &end_row);
            refine_row_range(&ctx, ts_from, ts_to, &start_row, &end_row);

            uint32_t row_cursor = 0;

            if (base_codec == CODEC_PLA_CHEB) {
                for (size_t s = 0; s < seg_count; s++) {
                    double c[4]; uint16_t L;
                    cheb_seg_read(seg_buf + s * CHEB_SEG_BYTES, c, &L);

                    uint32_t seg_start = row_cursor;
                    uint32_t seg_end   = row_cursor + (uint32_t)L - 1;
                    row_cursor += (uint32_t)L;

                    if (seg_end   < start_row) continue;
                    if (seg_start > end_row)   break;

                    uint32_t clip_s = (seg_start >= start_row) ? 0
                                    : (start_row - seg_start);
                    uint32_t clip_e = (uint32_t)L - 1;
                    if (seg_end > end_row) clip_e = end_row - seg_start;

                    double inv = (L > 1) ? 2.0 / (double)(L - 1) : 0.0;
                    for (uint32_t j = clip_s; j <= clip_e; j++) {
                        double u = (L > 1) ? inv * (double)j - 1.0 : 0.0;
                        double v = cheb3_eval(c, u);
                        if (!found || (want_min ? v < global_val : v > global_val)) {
                            global_val = v; found = true;
                        }
                    }
                }
            } else {
                for (size_t s = 0; s < seg_count; s++) {
                    double slope, offset; uint16_t L;
                    const uint8_t *sp = seg_buf + s * 18u;
                    memcpy(&slope,  sp,      8);
                    memcpy(&offset, sp + 8,  8);
                    memcpy(&L,      sp + 16, 2);

                    uint32_t seg_start = row_cursor;
                    uint32_t seg_end   = row_cursor + (uint32_t)L - 1;
                    row_cursor += (uint32_t)L;

                    if (seg_end   < start_row) continue;
                    if (seg_start > end_row)   break;

                    uint32_t clip_s = (seg_start >= start_row) ? 0 : (start_row - seg_start);
                    uint32_t clip_e = (uint32_t)L - 1;
                    if (seg_end > end_row) clip_e = end_row - seg_start;

                    double v_lo = offset + slope * (double)clip_s;
                    double v_hi = offset + slope * (double)clip_e;
                    double seg_val = want_min ? (v_lo < v_hi ? v_lo : v_hi)
                                              : (v_lo > v_hi ? v_lo : v_hi);
                    if (!found || (want_min ? seg_val < global_val
                                           : seg_val > global_val)) {
                        global_val = seg_val; found = true;
                    }
                }
            }
            ctx_release(&ctx);
        }
    }

    if (!found) return STRIQ_ERR_NOTFOUND;
    out->value         = global_val;
    out->error_bound   = p->col_epsilons[(uint32_t)ci];
    out->rows_scanned  = p->total_rows;
    out->pct_data_read = 100.0;
    out->pct_algebraic = 100.0;
    return STRIQ_OK;
}

/*
 * Uses Chan's parallel formula to merge (n, mean, M2) across blocks:
 *   delta = mean_b - mean_a
 *   M2    = M2_a + M2_b + delta² * n_a * n_b / (n_a + n_b)
 * For Chebyshev blocks: M2_seg = algebra_cheb_variance(c) * L  (Parseval).
 * For linear PLA:       M2_seg = algebra_linear_variance(slope, L) * L.
 * For stats-header codecs: M2 = sum_sq - sum²/n (from header).
 */
striq_status_t engine_query_variance(
    striq_query_engine_t *e,
    const char           *col_name,
    int64_t               ts_from,
    int64_t               ts_to,
    striq_query_result_t *out)
{
    if (!e || !col_name || !out) return STRIQ_ERR_PARAM;
    striq_block_provider_t *p = e->provider;

    int ci = find_col(p, col_name);
    if (ci < 0) return STRIQ_ERR_NOTFOUND;

    double   global_mean = 0.0;
    double   global_m2   = 0.0;
    uint64_t global_n    = 0;

    for (uint32_t bi = 0; bi < p->num_blocks; bi++) {
        striq_block_index_t bidx;
        if (p->get_block_index(p->ctx, bi, &bidx) != STRIQ_OK) continue;
        if (!block_in_range(&bidx, ts_from, ts_to)) continue;

        block_ctx_t ctx;
        striq_status_t rs = block_ctx_open(e, bi, &ctx);
        if (rs != STRIQ_OK) return rs;

        const uint8_t *seg_buf   = NULL;
        size_t         seg_count = 0;
        const uint8_t *raw_data  = NULL;
        size_t         raw_len   = 0;
        uint8_t        codec_byte = 0;
        rs = ctx_read_col_base(&ctx, (uint32_t)ci, &seg_buf, &seg_count,
                               &codec_byte, &raw_data, &raw_len);

        double   blk_mean = 0.0, blk_m2 = 0.0;
        uint64_t blk_n    = 0;

        if (rs == STRIQ_ERR_QUERY) {
            striq_codec_t base = CODEC_BASE(codec_byte);
            if (base == CODEC_RAW_STATS || base == CODEC_DECIMAL ||
                base == CODEC_QUANT8   || base == CODEC_QUANT16) {
                striq_raw_stats_hdr_t hdr;
                if (raw_stats_parse_hdr(raw_data, raw_len, &hdr) == STRIQ_OK
                    && hdr.count > 0) {
                    blk_n    = hdr.count;
                    blk_mean = hdr.sum / (double)hdr.count;
                    blk_m2   = hdr.sum_sq - hdr.sum * hdr.sum / (double)hdr.count;
                    if (blk_m2 < 0.0) blk_m2 = 0.0;
                }
            } else {
                /* RLE or unknown: decompress and use per-row Welford */
                double *vals = NULL;
                if (get_block_values(raw_data, raw_len, codec_byte,
                                     ctx.num_rows, &vals) == STRIQ_OK && vals) {
                    uint32_t r0 = 0, r1 = ctx.num_rows - 1;
                    if (ts_from != 0 || ts_to != 0) {
                        dod_find_range(ctx.idx, ctx.idx_count, ctx.num_rows,
                                       ts_from, ts_to, &r0, &r1);
                        refine_row_range(&ctx, ts_from, ts_to, &r0, &r1);
                    }
                    for (uint32_t r = r0; r <= r1; r++) {
                        blk_n++;
                        double delta = vals[r] - blk_mean;
                        blk_mean += delta / (double)blk_n;
                        blk_m2   += delta * (vals[r] - blk_mean);
                    }
                    free(vals);
                }
            }
        } else if (rs == STRIQ_OK) {
            striq_codec_t base_codec = CODEC_BASE(codec_byte);

            if (base_codec == CODEC_PLA_CHEB) {
                uint32_t row_cursor = 0;
                for (size_t s = 0; s < seg_count; s++) {
                    double c[4]; uint16_t L;
                    cheb_seg_read(seg_buf + s * CHEB_SEG_BYTES, c, &L);
                    uint32_t seg_s = row_cursor;
                    row_cursor += (uint32_t)L;
                    if (ts_from != 0 || ts_to != 0) {
                        if (seg_s > ctx.num_rows - 1) continue;
                    }
                    double seg_mean = c[0];
                    double seg_m2   = algebra_cheb_variance(c) * (double)L;
                    uint64_t new_n  = blk_n + (uint64_t)L;
                    double delta    = seg_mean - blk_mean;
                    blk_m2   = blk_m2 + seg_m2
                             + delta * delta * (double)blk_n * (double)L / (double)new_n;
                    blk_mean = (blk_mean * (double)blk_n + seg_mean * (double)L) / (double)new_n;
                    blk_n    = new_n;
                }
            } else {
                uint32_t row_cursor = 0;
                for (size_t s = 0; s < seg_count; s++) {
                    double slope, offset; uint16_t L;
                    const uint8_t *sp = seg_buf + s * 18u;
                    memcpy(&slope,  sp,      8);
                    memcpy(&offset, sp + 8,  8);
                    memcpy(&L,      sp + 16, 2);
                    uint32_t seg_s = row_cursor;
                    uint32_t seg_e = row_cursor + (uint32_t)L - 1;
                    row_cursor += (uint32_t)L;
                    (void)seg_s; (void)seg_e;
                    double seg_mean = offset + slope * (double)(L - 1) / 2.0;
                    double seg_m2   = algebra_linear_variance(slope, L) * (double)L;
                    uint64_t new_n  = blk_n + (uint64_t)L;
                    double delta    = seg_mean - blk_mean;
                    blk_m2   = blk_m2 + seg_m2
                             + delta * delta * (double)blk_n * (double)L / (double)new_n;
                    blk_mean = (blk_mean * (double)blk_n + seg_mean * (double)L) / (double)new_n;
                    blk_n    = new_n;
                }
            }
        }

        ctx_release(&ctx);

        if (blk_n == 0) continue;

        uint64_t new_n  = global_n + blk_n;
        double delta    = blk_mean - global_mean;
        global_m2   = global_m2 + blk_m2
                    + delta * delta * (double)global_n * (double)blk_n / (double)new_n;
        global_mean = (global_mean * (double)global_n + blk_mean * (double)blk_n) / (double)new_n;
        global_n    = new_n;
    }

    if (global_n == 0) return STRIQ_ERR_NOTFOUND;

    out->value         = global_m2 / (double)global_n;
    out->error_bound   = p->col_epsilons[(uint32_t)ci];
    out->rows_scanned  = global_n;
    out->pct_data_read = 100.0 * (double)global_n / (double)p->total_rows;
    out->pct_algebraic = 100.0;
    return STRIQ_OK;
}

striq_status_t engine_query_min(
    striq_query_engine_t *e,
    const char           *col_name,
    int64_t               ts_from,
    int64_t               ts_to,
    striq_query_result_t *out)
{ return engine_query_minmax(e, col_name, ts_from, ts_to, true, out); }

striq_status_t engine_query_max(
    striq_query_engine_t *e,
    const char           *col_name,
    int64_t               ts_from,
    int64_t               ts_to,
    striq_query_result_t *out)
{ return engine_query_minmax(e, col_name, ts_from, ts_to, false, out); }

striq_status_t engine_query_downsample(
    striq_query_engine_t *e,
    const char           *col_name,
    int64_t               ts_from,
    int64_t               ts_to,
    uint32_t              n_points,
    double               *out_values,
    int64_t              *out_ts)
{
    if (!e || !col_name || !out_values || n_points == 0) return STRIQ_ERR_PARAM;
    striq_block_provider_t *p = e->provider;

    int ci = find_col(p, col_name);
    if (ci < 0) return STRIQ_ERR_NOTFOUND;

    int64_t t_first = ts_from, t_last = ts_to;
    if (t_first == 0 || t_last == 0) {
        for (uint32_t bi = 0; bi < p->num_blocks; bi++) {
            striq_block_index_t bidx;
            if (p->get_block_index(p->ctx, bi, &bidx) != STRIQ_OK) continue;
            if (t_first == 0 || bidx.ts_first < t_first) t_first = bidx.ts_first;
            if (t_last  == 0 || bidx.ts_last  > t_last)  t_last  = bidx.ts_last;
        }
    }
    if (t_first == 0 && t_last == 0) return STRIQ_ERR_NOTFOUND;

    double time_step = (n_points > 1)
        ? (double)(t_last - t_first) / (double)(n_points - 1)
        : 0.0;

    for (uint32_t i = 0; i < n_points; i++) {
        out_values[i] = NAN;
        if (out_ts) out_ts[i] = t_first + (int64_t)(time_step * (double)i);
    }

    for (uint32_t bi = 0; bi < p->num_blocks; bi++) {
        striq_block_index_t bidx;
        if (p->get_block_index(p->ctx, bi, &bidx) != STRIQ_OK) continue;
        if (!block_in_range(&bidx, ts_from, ts_to)) continue;

        block_ctx_t ctx;
        if (block_ctx_open(e, bi, &ctx) != STRIQ_OK) continue;

        const uint8_t *seg_buf   = NULL;
        size_t         seg_count = 0;
        const uint8_t *raw_data  = NULL;
        size_t         raw_len   = 0;
        uint8_t        codec_byte = 0;
        striq_status_t rs = ctx_read_col_base(&ctx, (uint32_t)ci,
                                               &seg_buf, &seg_count,
                                               &codec_byte, &raw_data, &raw_len);

        /* Non-PLA codecs: decompress and pick nearest row */
        if (rs == STRIQ_ERR_QUERY) {
            striq_codec_t non_pla4 = CODEC_BASE(codec_byte);
            double *vals = NULL;
            get_block_values(raw_data, raw_len, codec_byte, ctx.num_rows, &vals);
            (void)non_pla4;
            if (vals) {
                for (uint32_t pi = 0; pi < n_points; pi++) {
                    int64_t sample_ts = t_first + (int64_t)(time_step * (double)pi);
                    if (sample_ts < bidx.ts_first || sample_ts > bidx.ts_last) continue;
                    double frac = (bidx.ts_last > bidx.ts_first)
                        ? (double)(sample_ts - bidx.ts_first)
                          / (double)(bidx.ts_last - bidx.ts_first) : 0.0;
                    uint32_t row = (uint32_t)(frac * (double)(ctx.num_rows - 1) + 0.5);
                    if (row >= ctx.num_rows) row = ctx.num_rows - 1;
                    out_values[pi] = vals[row];
                }
                free(vals);
            }
            ctx_release(&ctx);
            continue;
        }
        if (rs != STRIQ_OK) { ctx_release(&ctx); continue; }

        striq_codec_t base_codec = CODEC_BASE(codec_byte);
        size_t seg_stride = (base_codec == CODEC_PLA_CHEB) ? CHEB_SEG_BYTES : 18u;

        size_t max_segs = 8192;
        if (seg_count > max_segs) seg_count = max_segs;

        uint32_t *seg_row_start = malloc(seg_count * sizeof(uint32_t));
        if (!seg_row_start) { ctx_release(&ctx); continue; }

        {
            uint32_t rc = 0;
            for (size_t s = 0; s < seg_count; s++) {
                seg_row_start[s] = rc;
                uint16_t L;
                memcpy(&L, seg_buf + s * seg_stride + seg_stride - 2, 2);
                rc += (uint32_t)L;
            }
        }
        uint32_t total_block_rows = ctx.num_rows;

        for (uint32_t pi = 0; pi < n_points; pi++) {
            int64_t sample_ts = t_first + (int64_t)(time_step * (double)pi);
            if (sample_ts < bidx.ts_first || sample_ts > bidx.ts_last) continue;

            double frac = 0.0;
            if (bidx.ts_last > bidx.ts_first)
                frac = (double)(sample_ts - bidx.ts_first)
                     / (double)(bidx.ts_last - bidx.ts_first);
            uint32_t row = (uint32_t)(frac * (double)(total_block_rows - 1) + 0.5);
            if (row >= total_block_rows) row = total_block_rows - 1;

            size_t seg = 0;
            for (size_t s = 0; s + 1 < seg_count; s++) {
                uint16_t L;
                memcpy(&L, seg_buf + s * seg_stride + seg_stride - 2, 2);
                if (row < seg_row_start[s] + (uint32_t)L) { seg = s; break; }
                if (s + 1 == seg_count - 1) seg = seg_count - 1;
            }

            uint32_t local_row = (row >= seg_row_start[seg])
                               ? row - seg_row_start[seg] : 0;

            if (base_codec == CODEC_PLA_CHEB) {
                double c[4]; uint16_t L;
                cheb_seg_read(seg_buf + seg * CHEB_SEG_BYTES, c, &L);
                double u = (L > 1) ? 2.0 * (double)local_row / (double)(L - 1) - 1.0
                                   : 0.0;
                out_values[pi] = cheb3_eval(c, u);
            } else {
                double slope, offset;
                memcpy(&slope,  seg_buf + seg * 18u,     8);
                memcpy(&offset, seg_buf + seg * 18u + 8, 8);
                out_values[pi] = offset + slope * (double)local_row;
            }
        }

        free(seg_row_start);
        ctx_release(&ctx);
    }

    return STRIQ_OK;
}

static double eval_pla_at_row(
    const uint8_t *seg_buf, size_t seg_count,
    size_t stride, striq_codec_t codec, uint32_t row)
{
    uint32_t cursor = 0;
    for (size_t s = 0; s < seg_count; s++) {
        uint16_t L;
        memcpy(&L, seg_buf + s * stride + stride - 2, 2);
        if (row < cursor + (uint32_t)L) {
            uint32_t local = row - cursor;
            if (codec == CODEC_PLA_CHEB) {
                double c[4];
                cheb_seg_read(seg_buf + s * CHEB_SEG_BYTES, c, &L);
                double u = (L > 1) ? 2.0 * (double)local / (double)(L - 1) - 1.0 : 0.0;
                return cheb3_eval(c, u);
            }
            double slope, offset;
            memcpy(&slope,  seg_buf + s * 18u,     8);
            memcpy(&offset, seg_buf + s * 18u + 8, 8);
            return offset + slope * (double)local;
        }
        cursor += (uint32_t)L;
    }
    return NAN;
}

static inline bool codec_is_lossless(striq_codec_t c)
{
    return c == CODEC_RAW_STATS || c == CODEC_DECIMAL || c == CODEC_RLE;
}

striq_status_t engine_query_value_at(
    striq_query_engine_t *e,
    const uint32_t       *col_indices,
    uint32_t              n_cols,
    int64_t               timestamp_ns,
    double               *out_values,
    double               *out_errors)
{
    if (!e || !col_indices || !out_values || n_cols == 0) return STRIQ_ERR_PARAM;
    striq_block_provider_t *p = e->provider;

    for (uint32_t bi = 0; bi < p->num_blocks; bi++) {
        striq_block_index_t bidx;
        if (p->get_block_index(p->ctx, bi, &bidx) != STRIQ_OK) continue;
        if (!block_in_range(&bidx, timestamp_ns, timestamp_ns)) continue;

        block_ctx_t ctx;
        if (block_ctx_open(e, bi, &ctx) != STRIQ_OK) continue;

        uint32_t r0 = 0, r1 = ctx.num_rows - 1;
        dod_find_range(ctx.idx, ctx.idx_count, ctx.num_rows,
                       timestamp_ns, timestamp_ns, &r0, &r1);
        refine_row_range(&ctx, timestamp_ns, timestamp_ns, &r0, &r1);

        if (r0 > r1) { ctx_release(&ctx); continue; }

        uint32_t row = r0;
        for (uint32_t c = 0; c < n_cols; c++) {
            const uint8_t *seg_buf = NULL;
            size_t seg_count = 0;
            const uint8_t *raw_data = NULL;
            size_t raw_len = 0;
            uint8_t codec_byte = 0;

            striq_status_t rs = ctx_read_col_base(
                &ctx, col_indices[c],
                &seg_buf, &seg_count, &codec_byte, &raw_data, &raw_len);

            if (rs == STRIQ_OK) {
                striq_codec_t base = CODEC_BASE(codec_byte);
                size_t st = (base == CODEC_PLA_CHEB) ? CHEB_SEG_BYTES : 18u;
                out_values[c] = eval_pla_at_row(seg_buf, seg_count, st, base, row);
                if (out_errors)
                    out_errors[c] = p->col_epsilons[col_indices[c]];
            } else if (rs == STRIQ_ERR_QUERY) {
                double *vals = NULL;
                if (get_block_values(raw_data, raw_len, codec_byte,
                                     ctx.num_rows, &vals) == STRIQ_OK) {
                    out_values[c] = vals[row];
                    free(vals);
                } else {
                    out_values[c] = NAN;
                }
                if (out_errors) {
                    striq_codec_t base = CODEC_BASE(codec_byte);
                    out_errors[c] = codec_is_lossless(base)
                        ? 0.0 : p->col_epsilons[col_indices[c]];
                }
            } else {
                out_values[c] = NAN;
                if (out_errors) out_errors[c] = -1.0;
            }
        }
        ctx_release(&ctx);
        return STRIQ_OK;
    }
    return STRIQ_ERR_NOTFOUND;
}

striq_status_t engine_query_scan(
    striq_query_engine_t *e,
    const uint32_t       *col_indices,
    uint32_t              n_cols,
    int64_t               ts_from,
    int64_t               ts_to,
    double               *out_values,
    int64_t              *out_timestamps,
    uint32_t              max_rows,
    uint32_t             *out_num_rows)
{
    if (!e || !col_indices || !out_values || n_cols == 0 || max_rows == 0)
        return STRIQ_ERR_PARAM;
    striq_block_provider_t *p = e->provider;
    uint32_t written = 0;

    for (uint32_t bi = 0; bi < p->num_blocks && written < max_rows; bi++) {
        striq_block_index_t bidx;
        if (p->get_block_index(p->ctx, bi, &bidx) != STRIQ_OK) continue;
        if (!block_in_range(&bidx, ts_from, ts_to)) continue;

        block_ctx_t ctx;
        if (block_ctx_open(e, bi, &ctx) != STRIQ_OK) continue;

        uint32_t r0 = 0, r1 = ctx.num_rows - 1;
        if (ts_from != 0 || ts_to != 0) {
            dod_find_range(ctx.idx, ctx.idx_count, ctx.num_rows,
                           ts_from, ts_to, &r0, &r1);
            refine_row_range(&ctx, ts_from, ts_to, &r0, &r1);
        }
        if (r0 > r1) { ctx_release(&ctx); continue; }

        uint32_t take = r1 - r0 + 1;
        if (written + take > max_rows) take = max_rows - written;

        if (out_timestamps) {
            dod_decode_range(ctx.dod_stream, ctx.dod_len, ctx.num_rows,
                             ctx.idx, ctx.idx_count,
                             r0, r0 + take - 1, out_timestamps + written);
        }

        for (uint32_t c = 0; c < n_cols; c++) {
            const uint8_t *seg_buf = NULL;
            size_t seg_count = 0;
            const uint8_t *raw_data = NULL;
            size_t raw_len = 0;
            uint8_t codec_byte = 0;

            striq_status_t rs = ctx_read_col_base(
                &ctx, col_indices[c],
                &seg_buf, &seg_count, &codec_byte, &raw_data, &raw_len);

            if (rs == STRIQ_OK) {
                striq_codec_t base = CODEC_BASE(codec_byte);
                size_t st = (base == CODEC_PLA_CHEB) ? CHEB_SEG_BYTES : 18u;
                uint32_t cursor = 0;
                for (size_t s = 0; s < seg_count; s++) {
                    uint16_t L;
                    memcpy(&L, seg_buf + s * st + st - 2, 2);
                    uint32_t seg_s = cursor;
                    uint32_t seg_e = cursor + (uint32_t)L - 1;
                    cursor += (uint32_t)L;

                    if (seg_e < r0) continue;
                    if (seg_s >= r0 + take) break;

                    uint32_t from = (seg_s >= r0) ? seg_s : r0;
                    uint32_t to   = (seg_e < r0 + take - 1) ? seg_e : r0 + take - 1;

                    if (base == CODEC_PLA_CHEB) {
                        double coefs[4]; uint16_t sL;
                        cheb_seg_read(seg_buf + s * CHEB_SEG_BYTES, coefs, &sL);
                        double inv = (sL > 1) ? 2.0 / (double)(sL - 1) : 0.0;
                        for (uint32_t r = from; r <= to; r++) {
                            double u = (sL > 1) ? inv * (double)(r - seg_s) - 1.0 : 0.0;
                            out_values[(written + r - r0) * n_cols + c] = cheb3_eval(coefs, u);
                        }
                    } else {
                        double slope, offset;
                        memcpy(&slope,  seg_buf + s * 18u,     8);
                        memcpy(&offset, seg_buf + s * 18u + 8, 8);
                        for (uint32_t r = from; r <= to; r++)
                            out_values[(written + r - r0) * n_cols + c] =
                                offset + slope * (double)(r - seg_s);
                    }
                }
            } else if (rs == STRIQ_ERR_QUERY) {
                double *vals = NULL;
                if (get_block_values(raw_data, raw_len, codec_byte,
                                     ctx.num_rows, &vals) == STRIQ_OK) {
                    for (uint32_t r = 0; r < take; r++)
                        out_values[(written + r) * n_cols + c] = vals[r0 + r];
                    free(vals);
                } else {
                    for (uint32_t r = 0; r < take; r++)
                        out_values[(written + r) * n_cols + c] = NAN;
                }
            }
        }
        written += take;
        ctx_release(&ctx);
    }

    if (out_num_rows) *out_num_rows = written;
    return (written > 0) ? STRIQ_OK : STRIQ_ERR_NOTFOUND;
}
