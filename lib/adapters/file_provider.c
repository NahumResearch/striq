#include "file_provider.h"
#include <stdlib.h>

static striq_status_t fp_get_block_index(
    void *ctx, uint32_t idx, striq_block_index_t *out)
{
    striq_fmt_reader_t *fmt = (striq_fmt_reader_t *)ctx;
    if (idx >= fmt->num_blocks) return STRIQ_ERR_NOTFOUND;
    *out = fmt->block_index[idx];
    return STRIQ_OK;
}

static striq_status_t fp_get_block_data(
    void *ctx, uint32_t idx, const uint8_t **data, uint32_t *size)
{
    striq_fmt_reader_t *fmt = (striq_fmt_reader_t *)ctx;
    if (idx >= fmt->num_blocks) return STRIQ_ERR_NOTFOUND;

    uint32_t bsz = fmt->block_index[idx].block_size;
    uint8_t *buf = malloc(bsz);
    if (!buf) return STRIQ_ERR_MEMORY;

    size_t out_len = 0;
    striq_status_t s = fmt_reader_read_block_raw(fmt, idx, buf, bsz, &out_len);
    if (s != STRIQ_OK) { free(buf); return s; }

    *data = buf;
    *size = (uint32_t)out_len;
    return STRIQ_OK;
}

static void fp_release_block(void *ctx, uint32_t idx, const uint8_t *data)
{
    (void)ctx; (void)idx;
    free((void *)data);
}

static striq_status_t fp_get_col_stats(
    void *ctx, uint32_t idx, uint32_t col_idx, striq_col_stats_t *out)
{
    striq_fmt_reader_t *fmt = (striq_fmt_reader_t *)ctx;
    if (idx >= fmt->num_blocks || col_idx >= fmt->num_cols)
        return STRIQ_ERR_NOTFOUND;
    *out = fmt->block_stats[idx][col_idx];
    return STRIQ_OK;
}

striq_status_t file_provider_init(
    striq_file_provider_t *p,
    striq_fmt_reader_t    *fmt)
{
    if (!p || !fmt) return STRIQ_ERR_PARAM;
    p->fmt = fmt;

    p->base.ctx         = fmt;
    p->base.num_blocks  = fmt->num_blocks;
    p->base.num_cols    = fmt->num_cols;
    p->base.epsilon_b   = fmt->epsilon_b;
    p->base.total_rows  = fmt->total_rows;
    p->base.col_schemas = fmt->cols;
    for (uint32_t c = 0; c < fmt->num_cols && c < STRIQ_MAX_COLS; c++) {
        double ce = fmt->cols[c].epsilon_b;
        p->base.col_epsilons[c] = (ce > 0.0) ? ce : fmt->epsilon_b;
    }

    p->base.get_block_index = fp_get_block_index;
    p->base.get_block_data  = fp_get_block_data;
    p->base.release_block   = fp_release_block;
    p->base.get_col_stats   = fp_get_col_stats;
    return STRIQ_OK;
}
