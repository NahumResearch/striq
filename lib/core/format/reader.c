#include "reader.h"
#include "endian.h"
#include "../codecs/crc32.h"
#include <string.h>
#include <stdint.h>

static striq_status_t read_exact(striq_fmt_reader_t *r, uint8_t *buf, size_t n)
{
    return r->read_fn(buf, n, r->io_ctx);
}

static striq_status_t seek_to(striq_fmt_reader_t *r, int64_t offset)
{
    return r->seek_fn(offset, 0 /* SEEK_SET */, r->io_ctx);
}
static inline uint64_t read_u64_le(const uint8_t *b)
{
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v |= (uint64_t)b[i] << (i * 8);
    return v;
}
static inline int64_t read_i64_le(const uint8_t *b)
{
    return (int64_t)read_u64_le(b);
}
static inline double read_f64(const uint8_t *b)
{
    double v; memcpy(&v, b, 8); return v;
}

striq_status_t fmt_reader_open(
    striq_fmt_reader_t *r,
    striq_read_fn       read_fn,
    striq_seek_fn       seek_fn,
    void               *io_ctx,
    uint64_t            file_size)
{
    if (!r || !read_fn || !seek_fn) return STRIQ_ERR_PARAM;
    if (file_size < STRIQ_HEADER_SIZE + 8) return STRIQ_ERR_FORMAT;

    memset(r, 0, sizeof(*r));
    r->read_fn   = read_fn;
    r->seek_fn   = seek_fn;
    r->io_ctx    = io_ctx;
    r->file_size = file_size;

    STRIQ_TRY(seek_to(r, 0));
    uint8_t hdr[64];
    STRIQ_TRY(read_exact(r, hdr, 64));

    if (hdr[0] != 'S' || hdr[1] != 'T' || hdr[2] != 'R' ||
        hdr[3] != 'I' || hdr[4] != 'Q')
        return STRIQ_ERR_FORMAT;

    r->version = hdr[6];
    if (r->version < STRIQ_VERSION_MIN || r->version > STRIQ_VERSION)
        return STRIQ_ERR_VERSION;

    r->num_cols   = (uint32_t)hdr[8] | ((uint32_t)hdr[9] << 8);
    r->total_rows = read_u64_le(hdr + 14);
    r->epsilon_b  = read_f64(hdr + 22);
    r->ts_min     = read_i64_le(hdr + 30);
    r->ts_max     = read_i64_le(hdr + 38);

    for (uint32_t c = 0; c < r->num_cols && c < STRIQ_MAX_COLS; c++) {
        uint8_t nlen;
        STRIQ_TRY(read_exact(r, &nlen, 1));
        if (nlen > 63) return STRIQ_ERR_FORMAT;
        STRIQ_TRY(read_exact(r, (uint8_t *)r->cols[c].name, nlen));
        r->cols[c].name[nlen] = '\0';
        uint8_t ct, cd;
        STRIQ_TRY(read_exact(r, &ct, 1));
        STRIQ_TRY(read_exact(r, &cd, 1));
        r->cols[c].type  = (striq_coltype_t)ct;
        r->cols[c].codec = (striq_codec_t)cd;
        if (r->version >= 3) {
            uint8_t eb[8];
            STRIQ_TRY(read_exact(r, eb, 8));
            r->cols[c].epsilon_b = read_f64(eb);
        } else {
            r->cols[c].epsilon_b = 0.0;
        }
    }

    /* Last 8 bytes = footer_offset */
    STRIQ_TRY(seek_to(r, (int64_t)(file_size - 8)));
    uint8_t fo_buf[8];
    STRIQ_TRY(read_exact(r, fo_buf, 8));
    uint64_t footer_offset = read_u64_le(fo_buf);

    if (footer_offset >= file_size - 8) return STRIQ_ERR_FORMAT;
    STRIQ_TRY(seek_to(r, (int64_t)footer_offset));
    size_t footer_bytes = (size_t)(file_size - 8 - footer_offset);
    /* 8=offset, 4=block_size, 4=num_rows, 8=ts_first, 8=ts_last, then col stats */
    size_t per_block    = 8 + 4 + 4 + 8 + 8 + r->num_cols * (8 + 8 + 8 + 8 + 4);
    r->num_blocks       = (per_block > 0) ? (uint32_t)(footer_bytes / per_block) : 0;
    if (r->num_blocks > STRIQ_MAX_BLOCKS) r->num_blocks = STRIQ_MAX_BLOCKS;

    for (uint32_t bi = 0; bi < r->num_blocks; bi++) {
        uint8_t buf8[8]; uint8_t buf4[4];

        STRIQ_TRY(read_exact(r, buf8, 8));
        r->block_index[bi].file_offset = read_u64_le(buf8);

        STRIQ_TRY(read_exact(r, buf4, 4));
        r->block_index[bi].block_size = read_u32_le(buf4);

        STRIQ_TRY(read_exact(r, buf4, 4));
        r->block_index[bi].num_rows = read_u32_le(buf4);

        STRIQ_TRY(read_exact(r, buf8, 8));
        r->block_index[bi].ts_first = read_i64_le(buf8);

        STRIQ_TRY(read_exact(r, buf8, 8));
        r->block_index[bi].ts_last = read_i64_le(buf8);

        for (uint32_t c = 0; c < r->num_cols; c++) {
            STRIQ_TRY(read_exact(r, buf8, 8));
            r->block_stats[bi][c].min = read_f64(buf8);
            STRIQ_TRY(read_exact(r, buf8, 8));
            r->block_stats[bi][c].max = read_f64(buf8);
            STRIQ_TRY(read_exact(r, buf8, 8));
            r->block_stats[bi][c].sum = read_f64(buf8);
            STRIQ_TRY(read_exact(r, buf8, 8));
            r->block_stats[bi][c].count = read_u64_le(buf8);
            STRIQ_TRY(read_exact(r, buf4, 4));
            r->block_stats[bi][c].num_segments = read_u32_le(buf4);
        }
    }
    r->total_rows = 0;
    if (r->num_blocks > 0) {
        r->ts_min = r->block_index[0].ts_first;
        r->ts_max = r->block_index[0].ts_last;
        for (uint32_t bi = 0; bi < r->num_blocks; bi++) {
            r->total_rows += r->block_index[bi].num_rows;
            if (r->block_index[bi].ts_first < r->ts_min)
                r->ts_min = r->block_index[bi].ts_first;
            if (r->block_index[bi].ts_last > r->ts_max)
                r->ts_max = r->block_index[bi].ts_last;
        }
    }

    return STRIQ_OK;
}

striq_status_t fmt_reader_read_block_raw(
    striq_fmt_reader_t *r,
    uint32_t            block_idx,
    uint8_t            *buf,
    size_t              buf_cap,
    size_t             *out_len)
{
    if (!r || !buf || !out_len) return STRIQ_ERR_PARAM;
    if (block_idx >= r->num_blocks) return STRIQ_ERR_NOTFOUND;

    uint32_t bsz = r->block_index[block_idx].block_size;
    if (bsz > buf_cap) return STRIQ_ERR_MEMORY;

    STRIQ_TRY(seek_to(r, (int64_t)r->block_index[block_idx].file_offset));
    STRIQ_TRY(read_exact(r, buf, bsz));
    *out_len = bsz;

    if (r->version >= 2 && bsz >= 4) {
        uint32_t stored = (uint32_t)buf[bsz-4]
            | ((uint32_t)buf[bsz-3] << 8)
            | ((uint32_t)buf[bsz-2] << 16)
            | ((uint32_t)buf[bsz-1] << 24);
        uint32_t computed = crc32c(buf, bsz - 4);
        if (computed != stored) return STRIQ_ERR_CORRUPT;
    }

    return STRIQ_OK;
}
