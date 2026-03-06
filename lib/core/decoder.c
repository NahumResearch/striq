#include "decoder.h"
#include "format/endian.h"
#include "codecs/dod.h"
#include "codecs/pla.h"
#include "codecs/chebyshev.h"
#include "codecs/raw_stats.h"
#include "codecs/rle_codec.h"
#include "codecs/decimal.h"
#include "codecs/quant.h"
#include <string.h>
#include <stdlib.h>

striq_status_t decoder_init(
    striq_decoder_t *d,
    striq_read_fn    read_fn,
    striq_seek_fn    seek_fn,
    void            *io_ctx,
    uint64_t         file_size)
{
    if (!d) return STRIQ_ERR_PARAM;
    memset(d, 0, sizeof(*d));
    STRIQ_TRY(fmt_reader_open(&d->fmt, read_fn, seek_fn, io_ctx, file_size));
    d->epsilon_b = d->fmt.epsilon_b;
    return STRIQ_OK;
}

static striq_status_t read_block_buf(
    striq_decoder_t *d, uint32_t bi,
    uint8_t **out_buf, size_t *out_len)
{
    uint32_t bsz = d->fmt.block_index[bi].block_size;
    uint8_t *buf = malloc(bsz);
    if (!buf) return STRIQ_ERR_MEMORY;
    striq_status_t s = fmt_reader_read_block_raw(&d->fmt, bi, buf, bsz, out_len);
    if (s != STRIQ_OK) { free(buf); return s; }
    *out_buf = buf;
    return STRIQ_OK;
}

#define rd_u32 read_u32_le

striq_status_t decoder_read_timestamps(
    striq_decoder_t *d,
    uint32_t         block_idx,
    int64_t         *out,
    size_t           out_cap)
{
    if (!d || !out) return STRIQ_ERR_PARAM;
    if (block_idx >= d->fmt.num_blocks) return STRIQ_ERR_NOTFOUND;

    uint32_t n = d->fmt.block_index[block_idx].num_rows;
    if (out_cap < n) return STRIQ_ERR_MEMORY;

    uint8_t *buf; size_t blen;
    STRIQ_TRY(read_block_buf(d, block_idx, &buf, &blen));

    size_t pos = 24;
    uint32_t ts_data_len = rd_u32(buf + pos); pos += 4;
    (void)ts_data_len;

    uint16_t idx_count = (uint16_t)buf[pos] | ((uint16_t)buf[pos+1] << 8);
    pos += 2;
    pos += (size_t)idx_count * 16;

    uint32_t dod_len = rd_u32(buf + pos); pos += 4;
    striq_status_t s = dod_decode(buf + pos, dod_len, n, out);
    free(buf);
    return s;
}

striq_status_t decoder_read_column(
    striq_decoder_t *d,
    uint32_t         block_idx,
    uint32_t         col_idx,
    double          *out,
    size_t           out_cap)
{
    if (!d || !out) return STRIQ_ERR_PARAM;
    if (block_idx >= d->fmt.num_blocks) return STRIQ_ERR_NOTFOUND;
    if (col_idx >= d->fmt.num_cols) return STRIQ_ERR_PARAM;

    uint32_t n = d->fmt.block_index[block_idx].num_rows;
    if (out_cap < n) return STRIQ_ERR_MEMORY;

    uint8_t *buf; size_t blen;
    STRIQ_TRY(read_block_buf(d, block_idx, &buf, &blen));

    size_t pos = 24;
    uint32_t ts_len = rd_u32(buf + pos); pos += 4 + ts_len;

    striq_status_t s = STRIQ_OK;
    for (uint32_t c = 0; c <= col_idx; c++) {
        if (pos >= blen) { s = STRIQ_ERR_FORMAT; goto done; }

        uint8_t codec_byte = buf[pos++];
        striq_codec_t base_codec = CODEC_BASE(codec_byte);
        uint32_t base_len  = rd_u32(buf + pos); pos += 4;

        if (c == col_idx) {
            const uint8_t *base_data  = buf + pos;
            pos += base_len;
            uint32_t resid_len = rd_u32(buf + pos); pos += 4;
            const uint8_t *resid_data = buf + pos;

            if (base_codec == CODEC_PLA_LINEAR) {
                size_t seg_count = base_len / 18u;
                s = pla_decode(base_data, seg_count, false,
                               resid_data, resid_len, n, out);
            } else if (base_codec == CODEC_PLA_CHEB) {
                size_t seg_count = base_len / CHEB_SEG_BYTES;
                s = pla_decode(base_data, seg_count, true,
                               resid_data, resid_len, n, out);
            } else if (base_codec == CODEC_RLE) {
                s = rle_decode(base_data, base_len, out, n);
            } else if (base_codec == CODEC_RAW_STATS) {
                s = raw_stats_decode(base_data, base_len, out, n);
            } else if (base_codec == CODEC_DECIMAL) {
                s = decimal_decode(base_data, base_len, out, n);
            } else if (base_codec == CODEC_QUANT16) {
                s = quant_decode(base_data, base_len, 16, out, n);
            } else if (base_codec == CODEC_QUANT8) {
                s = quant_decode(base_data, base_len, 8, out, n);
            } else {
                s = STRIQ_ERR_FORMAT;
            }
            goto done;
        }

        pos += base_len;
        uint32_t resid_len = rd_u32(buf + pos); pos += 4 + resid_len;
    }

done:
    free(buf);
    return s;
}
