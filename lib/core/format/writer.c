#include "writer.h"
#include "../codecs/crc32.h"
#include <string.h>
#include <stdint.h>

static striq_status_t emit(striq_fmt_writer_t *w, const uint8_t *data, size_t len)
{
    STRIQ_TRY(w->write_fn(data, len, w->write_ctx));
    w->bytes_written += len;
    if (w->in_block)
        w->block_crc = crc32c_update(w->block_crc, data, len);
    return STRIQ_OK;
}

static striq_status_t emit_u32(striq_fmt_writer_t *w, uint32_t v)
{
    uint8_t b[4] = {
        (uint8_t)(v),
        (uint8_t)(v >> 8),
        (uint8_t)(v >> 16),
        (uint8_t)(v >> 24)
    };
    return emit(w, b, 4);
}

static striq_status_t emit_u64(striq_fmt_writer_t *w, uint64_t v)
{
    uint8_t b[8];
    for (int i = 0; i < 8; i++) b[i] = (uint8_t)(v >> (i * 8));
    return emit(w, b, 8);
}

static striq_status_t emit_f64(striq_fmt_writer_t *w, double v)
{
    return emit(w, (const uint8_t *)&v, 8);
}

static striq_status_t emit_i64(striq_fmt_writer_t *w, int64_t v)
{
    return emit_u64(w, (uint64_t)v);
}

striq_status_t fmt_writer_init(
    striq_fmt_writer_t       *w,
    const striq_col_schema_t *cols,
    uint32_t                  num_cols,
    double                    epsilon_b,
    striq_write_fn            write_fn,
    void                     *write_ctx)
{
    if (!w || !cols || !write_fn) return STRIQ_ERR_PARAM;
    if (num_cols == 0 || num_cols > STRIQ_MAX_COLS) return STRIQ_ERR_PARAM;

    memset(w, 0, sizeof(*w));
    memcpy(w->cols, cols, num_cols * sizeof(striq_col_schema_t));
    w->num_cols   = num_cols;
    w->epsilon_b  = epsilon_b;
    w->write_fn   = write_fn;
    w->write_ctx  = write_ctx;
    w->ts_min     = INT64_MAX;
    w->ts_max     = INT64_MIN;
    return STRIQ_OK;
}

striq_status_t fmt_writer_write_header(striq_fmt_writer_t *w)
{
    if (!w) return STRIQ_ERR_PARAM;

    /* Magic + version (6 + 2 bytes) */
    uint8_t magic[6] = {'S','T','R','I','Q','\0'};
    STRIQ_TRY(emit(w, magic, 6));
    uint8_t ver[2] = { STRIQ_VERSION, 0 };
    STRIQ_TRY(emit(w, ver, 2));

    uint8_t nc[2] = { (uint8_t)(w->num_cols), (uint8_t)(w->num_cols >> 8) };
    STRIQ_TRY(emit(w, nc, 2));
    STRIQ_TRY(emit_u32(w, 0)); /* filled in footer */

    STRIQ_TRY(emit_u64(w, 0));
    STRIQ_TRY(emit_f64(w, w->epsilon_b));
    STRIQ_TRY(emit_i64(w, 0));
    STRIQ_TRY(emit_i64(w, 0));

    /* Pad to 64 bytes: used so far = 6+2+2+4+8+8+8+8 = 46; need 18 more */
    uint8_t pad[18];
    memset(pad, 0, sizeof(pad));
    STRIQ_TRY(emit(w, pad, 18));

    /* Column descriptors (v3): name_len u8, name[], col_type u8, codec u8, epsilon_b f64 */
    for (uint32_t c = 0; c < w->num_cols; c++) {
        size_t slen = strlen(w->cols[c].name);
        uint8_t nlen = (uint8_t)(slen > 63 ? 63 : slen);
        STRIQ_TRY(emit(w, &nlen, 1));
        STRIQ_TRY(emit(w, (const uint8_t *)w->cols[c].name, nlen));
        uint8_t ct = (uint8_t)w->cols[c].type;
        uint8_t cd = (uint8_t)w->cols[c].codec;
        STRIQ_TRY(emit(w, &ct, 1));
        STRIQ_TRY(emit(w, &cd, 1));
        STRIQ_TRY(emit_f64(w, w->col_epsilons[c]));
    }

    return STRIQ_OK;
}

striq_status_t fmt_writer_write_block(
    striq_fmt_writer_t       *w,
    const striq_block_data_t *blk)
{
    if (!w || !blk) return STRIQ_ERR_PARAM;
    if (w->num_blocks >= STRIQ_MAX_BLOCKS) return STRIQ_ERR_MEMORY;

    uint64_t block_start = w->bytes_written;

    /* Start CRC accumulation: init to 0xFFFFFFFF (we finalize at end of block) */
    w->block_crc = 0xFFFFFFFFu;
    w->in_block  = 1;

    /* block_size placeholder u32 — we will NOT seek back (streaming).
       Instead write 0 now and store the real size in the footer only. */
    STRIQ_TRY(emit_u32(w, 0));
    STRIQ_TRY(emit_u32(w, blk->num_rows));
    STRIQ_TRY(emit_i64(w, blk->ts_first));
    STRIQ_TRY(emit_i64(w, blk->ts_last));

    STRIQ_TRY(emit_u32(w, (uint32_t)blk->ts_len));
    STRIQ_TRY(emit(w, blk->ts_data, blk->ts_len));

    for (uint32_t c = 0; c < w->num_cols; c++) {
        uint8_t codec_byte = (uint8_t)blk->col[c].codec;
        STRIQ_TRY(emit(w, &codec_byte, 1));
        STRIQ_TRY(emit_u32(w, (uint32_t)blk->col[c].base_len));
        if (blk->col[c].base_len > 0)
            STRIQ_TRY(emit(w, blk->col[c].base_data, blk->col[c].base_len));
        STRIQ_TRY(emit_u32(w, (uint32_t)blk->col[c].resid_len));
        if (blk->col[c].resid_len > 0)
            STRIQ_TRY(emit(w, blk->col[c].resid_data, blk->col[c].resid_len));
    }

    /* Finalize CRC and append as 4 bytes (v2 format) */
    w->in_block = 0;
    uint32_t final_crc = w->block_crc ^ 0xFFFFFFFFu;
    uint8_t crc_bytes[4] = {
        (uint8_t)(final_crc),
        (uint8_t)(final_crc >> 8),
        (uint8_t)(final_crc >> 16),
        (uint8_t)(final_crc >> 24)
    };
    /* Write CRC without including it in next block's CRC */
    STRIQ_TRY(w->write_fn(crc_bytes, 4, w->write_ctx));
    w->bytes_written += 4;

    uint64_t block_end  = w->bytes_written;
    uint32_t block_size = (uint32_t)(block_end - block_start);

    uint32_t bi = w->num_blocks;
    w->block_index[bi].file_offset = block_start;
    w->block_index[bi].block_size  = block_size;
    w->block_index[bi].num_rows    = blk->num_rows;
    w->block_index[bi].ts_first    = blk->ts_first;
    w->block_index[bi].ts_last     = blk->ts_last;

    for (uint32_t c = 0; c < w->num_cols; c++)
        w->block_stats[bi][c] = blk->col[c].stats;

    w->num_blocks++;
    w->total_rows += blk->num_rows;

    if (blk->ts_first < w->ts_min) w->ts_min = blk->ts_first;
    if (blk->ts_last  > w->ts_max) w->ts_max = blk->ts_last;

    return STRIQ_OK;
}

striq_status_t fmt_writer_write_footer(striq_fmt_writer_t *w)
{
    if (!w) return STRIQ_ERR_PARAM;

    uint64_t footer_offset = w->bytes_written;

    for (uint32_t bi = 0; bi < w->num_blocks; bi++) {
        STRIQ_TRY(emit_u64(w, w->block_index[bi].file_offset));
        STRIQ_TRY(emit_u32(w, w->block_index[bi].block_size));  /* needed by reader */
        STRIQ_TRY(emit_u32(w, w->block_index[bi].num_rows));
        STRIQ_TRY(emit_i64(w, w->block_index[bi].ts_first));
        STRIQ_TRY(emit_i64(w, w->block_index[bi].ts_last));

        for (uint32_t c = 0; c < w->num_cols; c++) {
            striq_col_stats_t *st = &w->block_stats[bi][c];
            STRIQ_TRY(emit_f64(w, st->min));
            STRIQ_TRY(emit_f64(w, st->max));
            STRIQ_TRY(emit_f64(w, st->sum));
            STRIQ_TRY(emit_u64(w, st->count));
            STRIQ_TRY(emit_u32(w, st->num_segments));
        }
    }

    /* Last 8 bytes: footer_offset */
    STRIQ_TRY(emit_u64(w, footer_offset));

    return STRIQ_OK;
}
