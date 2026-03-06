#include "encoder.h"
#include "codecs/dod.h"
#include "codecs/pla.h"
#include "codecs/chebyshev.h"
#include "codecs/raw_stats.h"
#include "codecs/rle_codec.h"
#include "codecs/decimal.h"
#include "codecs/quant.h"
#include "routing/router.h"
#include <string.h>
#include <stdbool.h>

static striq_status_t flush_block(striq_encoder_t *e);

striq_status_t encoder_init(
    striq_encoder_t          *e,
    const striq_col_schema_t *cols,
    uint32_t                  num_cols,
    const striq_opts_t       *opts,
    striq_write_fn            write_fn,
    void                     *write_ctx)
{
    if (!e || !cols) return STRIQ_ERR_PARAM;
    memset(e, 0, sizeof(*e));

    double global_eps = opts ? opts->epsilon : 0.0;
    e->epsilon_b     = global_eps;
    e->col_skip      = opts ? opts->col_skip : 0;
    e->codec_decided = false;

    striq_col_schema_t enriched[STRIQ_MAX_COLS];
    memcpy(enriched, cols, num_cols * sizeof(striq_col_schema_t));
    for (uint32_t c = 0; c < num_cols; c++) {
        double col_eps = opts ? opts->col_epsilon[c] : 0.0;
        e->col_epsilons[c]      = col_eps;
        enriched[c].epsilon_b   = col_eps;
        e->fmt.col_epsilons[c]  = col_eps;
    }

    STRIQ_TRY(arena_init(&e->arena, e->arena_mem, sizeof(e->arena_mem)));
    STRIQ_TRY(fmt_writer_init(&e->fmt, enriched, num_cols, global_eps, write_fn, write_ctx));
    /* Copy col_epsilons into the writer state after init (memset in init clears it) */
    for (uint32_t c = 0; c < num_cols; c++)
        e->fmt.col_epsilons[c] = e->col_epsilons[c];
    STRIQ_TRY(fmt_writer_write_header(&e->fmt));
    return STRIQ_OK;
}

striq_status_t encoder_add_row(
    striq_encoder_t *e,
    int64_t          timestamp_ns,
    const double    *values,
    size_t           num_values)
{
    if (!e || !values) return STRIQ_ERR_PARAM;
    if (num_values != e->fmt.num_cols) return STRIQ_ERR_PARAM;

    if (e->buf_rows >= ENCODER_MAX_ROWS_PER_BLOCK)
        STRIQ_TRY(flush_block(e));

    e->ts_buf[e->buf_rows] = timestamp_ns;
    for (uint32_t c = 0; c < e->fmt.num_cols; c++)
        e->val_buf[c][e->buf_rows] = values[c];
    e->buf_rows++;
    return STRIQ_OK;
}

static striq_status_t flush_block(striq_encoder_t *e)
{
    if (e->buf_rows == 0) return STRIQ_OK;

    uint32_t n  = e->buf_rows;
    uint32_t nc = e->fmt.num_cols;
    arena_reset(&e->arena);

    if (!e->codec_decided) {
        for (uint32_t c = 0; c < nc; c++) {
            if (e->col_skip & (UINT64_C(1) << c)) {
                e->col_codec[c] = CODEC_RAW_STATS;
            } else {
                uint32_t sample = (n < STRIQ_ROUTE_SAMPLE) ? n : STRIQ_ROUTE_SAMPLE;
                double col_eps = (e->col_epsilons[c] > 0.0) ? e->col_epsilons[c] : e->epsilon_b;
                STRIQ_TRY(router_select(e->val_buf[c], sample, col_eps, &e->col_codec[c]));
            }
            e->fmt.cols[c].codec = e->col_codec[c];
        }
        e->codec_decided = true;
    }

    size_t dod_cap = (size_t)n * 10 + 16;
    uint8_t *dod_stream = arena_alloc(&e->arena, dod_cap);
    if (!dod_stream) return STRIQ_ERR_MEMORY;

    striq_ts_index_entry_t idx_entries[STRIQ_TS_INDEX_MAX_ENTRIES];
    uint16_t idx_count = 0;
    size_t   dod_len   = 0;
    STRIQ_TRY(dod_encode_indexed(e->ts_buf, n,
                                  dod_stream, dod_cap, &dod_len,
                                  idx_entries, &idx_count));

    size_t ts_buf_cap = 2 + (size_t)idx_count * 16 + 4 + dod_len;
    uint8_t *ts_encoded = arena_alloc(&e->arena, ts_buf_cap);
    if (!ts_encoded) return STRIQ_ERR_MEMORY;

    size_t tp = 0;
    ts_encoded[tp++] = (uint8_t)(idx_count & 0xFF);
    ts_encoded[tp++] = (uint8_t)(idx_count >> 8);
    for (uint16_t ix = 0; ix < idx_count; ix++) {
        uint64_t ts_raw = (uint64_t)idx_entries[ix].ts;
        for (int b = 0; b < 8; b++) ts_encoded[tp++] = (uint8_t)(ts_raw >> (b*8));
        uint32_t ro = idx_entries[ix].row_offset;
        for (int b = 0; b < 4; b++) ts_encoded[tp++] = (uint8_t)(ro >> (b*8));
        uint32_t bo = idx_entries[ix].byte_offset;
        for (int b = 0; b < 4; b++) ts_encoded[tp++] = (uint8_t)(bo >> (b*8));
    }
    uint32_t dod_len32 = (uint32_t)dod_len;
    for (int b = 0; b < 4; b++) ts_encoded[tp++] = (uint8_t)(dod_len32 >> (b*8));
    for (size_t i = 0; i < dod_len; i++) ts_encoded[tp++] = dod_stream[i];
    size_t ts_len = tp;

    striq_block_data_t blk;
    memset(&blk, 0, sizeof(blk));
    blk.ts_first = e->ts_buf[0];
    blk.ts_last  = e->ts_buf[n - 1];
    blk.num_rows = n;
    blk.ts_data  = ts_encoded;
    blk.ts_len   = ts_len;

    for (uint32_t c = 0; c < nc; c++) {
        /* val_buf is column-major: e->val_buf[c] is contiguous — no copy needed */
        const double *col_vals = e->val_buf[c];

        blk.col[c].codec = e->col_codec[c];

        double mn = col_vals[0], mx = col_vals[0], sum = 0.0;
        for (uint32_t i = 0; i < n; i++) {
            if (col_vals[i] < mn) mn = col_vals[i];
            if (col_vals[i] > mx) mx = col_vals[i];
            sum += col_vals[i];
        }
        blk.col[c].stats.min   = mn;
        blk.col[c].stats.max   = mx;
        blk.col[c].stats.sum   = sum;
        blk.col[c].stats.count = n;

        if (e->col_codec[c] == CODEC_RLE) {
            size_t rle_cap = 1 + (size_t)RLE_MAX_UNIQUE * 8 + 4
                           + (size_t)n * 12 + 8;
            uint8_t *rle_buf = arena_alloc(&e->arena, rle_cap);
            if (!rle_buf) return STRIQ_ERR_MEMORY;

            size_t rle_len = 0;
            striq_status_t rs = rle_encode(col_vals, n,
                                            rle_buf, rle_cap, &rle_len);
            if (rs != STRIQ_OK) {
                goto raw_stats_fallback;
            }

            blk.col[c].codec      = CODEC_RLE;
            blk.col[c].base_data  = rle_buf;
            blk.col[c].base_len   = rle_len;
            blk.col[c].resid_data = NULL;
            blk.col[c].resid_len  = 0;

        } else if (e->col_codec[c] == CODEC_PLA_LINEAR ||
                   e->col_codec[c] == CODEC_PLA_CHEB) {
            size_t seg_cap   = (n + 1) * CHEB_SEG_BYTES + 8;
            size_t resid_cap = n * 12 + 8;
            uint8_t *seg_buf   = arena_alloc(&e->arena, seg_cap);
            uint8_t *resid_buf = arena_alloc(&e->arena, resid_cap);
            if (!seg_buf || !resid_buf) return STRIQ_ERR_MEMORY;

            /* CODEC_PLA_CHEB: cheb_threshold=65535 → always try Chebyshev */
            uint32_t cheb_thresh = (e->col_codec[c] == CODEC_PLA_CHEB) ? 65535u : 32u;
            double col_eps = (e->col_epsilons[c] > 0.0) ? e->col_epsilons[c] : e->epsilon_b;

            size_t seg_count = 0, resid_len = 0;
            bool   used_cheb = false;
            STRIQ_TRY(pla_encode(col_vals, n, col_eps,
                                 cheb_thresh,
                                 seg_buf, seg_cap,
                                 &seg_count, &used_cheb,
                                 resid_buf, resid_cap, &resid_len));

            size_t seg_stride = used_cheb ? CHEB_SEG_BYTES : 18u;
            blk.col[c].codec      = used_cheb ? CODEC_PLA_CHEB : CODEC_PLA_LINEAR;
            blk.col[c].base_data  = seg_buf;
            blk.col[c].base_len   = seg_count * seg_stride;
            blk.col[c].resid_data = resid_buf;
            blk.col[c].resid_len  = resid_len;
            blk.col[c].stats.num_segments = (uint32_t)seg_count;

        } else if (e->col_codec[c] == CODEC_DECIMAL) {
            size_t dec_cap = DECIMAL_TOTAL_HDR_BYTES + (size_t)n * 4 + 8;
            uint8_t *dec_buf = arena_alloc(&e->arena, dec_cap);
            if (!dec_buf) return STRIQ_ERR_MEMORY;

            size_t dec_len = decimal_encode(col_vals, n, dec_buf, dec_cap);
            if (dec_len == 0) goto raw_stats_fallback;

            blk.col[c].codec      = CODEC_DECIMAL;
            blk.col[c].base_data  = dec_buf;
            blk.col[c].base_len   = dec_len;
            blk.col[c].resid_data = NULL;
            blk.col[c].resid_len  = 0;

        } else if (e->col_codec[c] == CODEC_QUANT16) {
            size_t q_cap = QUANT_TOTAL_HDR_BYTES + (size_t)n * 2 + 8;
            uint8_t *q_buf = arena_alloc(&e->arena, q_cap);
            if (!q_buf) return STRIQ_ERR_MEMORY;

            size_t q_len = quant_encode(col_vals, n, 16, q_buf, q_cap);
            if (q_len == 0) goto raw_stats_fallback;

            blk.col[c].codec      = CODEC_QUANT16;
            blk.col[c].base_data  = q_buf;
            blk.col[c].base_len   = q_len;
            blk.col[c].resid_data = NULL;
            blk.col[c].resid_len  = 0;

        } else if (e->col_codec[c] == CODEC_QUANT8) {
            size_t q_cap = QUANT_TOTAL_HDR_BYTES + (size_t)n + 8;
            uint8_t *q_buf = arena_alloc(&e->arena, q_cap);
            if (!q_buf) return STRIQ_ERR_MEMORY;

            size_t q_len = quant_encode(col_vals, n, 8, q_buf, q_cap);
            if (q_len == 0) goto raw_stats_fallback;

            blk.col[c].codec      = CODEC_QUANT8;
            blk.col[c].base_data  = q_buf;
            blk.col[c].base_len   = q_len;
            blk.col[c].resid_data = NULL;
            blk.col[c].resid_len  = 0;

        } else {
raw_stats_fallback:;
            size_t rs_cap = RAW_STATS_HDR_SIZE + (size_t)n * sizeof(double) + 8;
            uint8_t *rs_buf = arena_alloc(&e->arena, rs_cap);
            if (!rs_buf) return STRIQ_ERR_MEMORY;

            size_t rs_len = raw_stats_encode(col_vals, n, rs_buf, rs_cap);
            if (rs_len == 0) return STRIQ_ERR_CODEC;

            blk.col[c].codec      = CODEC_RAW_STATS;
            blk.col[c].base_data  = rs_buf;
            blk.col[c].base_len   = rs_len;
            blk.col[c].resid_data = NULL;
            blk.col[c].resid_len  = 0;
        }
    }

    STRIQ_TRY(fmt_writer_write_block(&e->fmt, &blk));
    e->buf_rows = 0;
    return STRIQ_OK;
}

striq_status_t encoder_close(striq_encoder_t *e)
{
    if (!e) return STRIQ_ERR_PARAM;
    STRIQ_TRY(flush_block(e));
    STRIQ_TRY(fmt_writer_write_footer(&e->fmt));
    return STRIQ_OK;
}
