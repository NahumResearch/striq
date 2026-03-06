#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "../lib/core/format/writer.h"
#include "../lib/core/format/reader.h"

typedef struct { uint8_t *buf; size_t cap; size_t size; size_t pos; } mem_ctx_t;

static striq_status_t mem_write(const uint8_t *data, size_t len, void *ctx)
{
    mem_ctx_t *m = (mem_ctx_t *)ctx;
    if (m->size + len > m->cap) return STRIQ_ERR_MEMORY;
    memcpy(m->buf + m->size, data, len);
    m->size += len;
    return STRIQ_OK;
}
static striq_status_t mem_read(uint8_t *buf, size_t len, void *ctx)
{
    mem_ctx_t *m = (mem_ctx_t *)ctx;
    if (m->pos + len > m->size) return STRIQ_ERR_IO;
    memcpy(buf, m->buf + m->pos, len);
    m->pos += len;
    return STRIQ_OK;
}
static striq_status_t mem_seek(int64_t offset, int whence, void *ctx)
{
    mem_ctx_t *m = (mem_ctx_t *)ctx;
    size_t new_pos;
    if (whence == 0)       new_pos = (size_t)offset;
    else if (whence == 1)  new_pos = m->pos + (size_t)offset;
    else                   new_pos = m->size + (size_t)offset;
    if (new_pos > m->size) return STRIQ_ERR_IO;
    m->pos = new_pos;
    return STRIQ_OK;
}

static void test_write_read_roundtrip(void)
{
    static uint8_t mem[4 * 1024 * 1024];
    mem_ctx_t wctx = { mem, sizeof(mem), 0, 0 };

    striq_col_schema_t cols[2] = {
        { "temperature", COL_FLOAT64, CODEC_PLA_LINEAR, 0.0 },
        { "humidity",    COL_FLOAT64, CODEC_RAW_STATS,  0.0 },
    };

    static striq_fmt_writer_t w;
    assert(fmt_writer_init(&w, cols, 2, 0.01, mem_write, &wctx) == STRIQ_OK);
    assert(fmt_writer_write_header(&w) == STRIQ_OK);

    /* Build a minimal block */
    static uint8_t ts_data[32];
    static uint8_t base0[128], res0[256], base1[256];
    uint16_t seg_len = 4;
    double slope = 0.5, offset_v = 20.0;
    memcpy(base0,      &slope,   8);
    memcpy(base0 + 8,  &offset_v, 8);
    memcpy(base0 + 16, &seg_len, 2);

    striq_block_data_t blk;
    memset(&blk, 0, sizeof(blk));
    blk.ts_first = 1000; blk.ts_last = 4000; blk.num_rows = 4;
    blk.ts_data  = ts_data; blk.ts_len = 8;
    blk.col[0].codec      = CODEC_PLA_LINEAR;
    blk.col[0].base_data  = base0; blk.col[0].base_len = 18;
    blk.col[0].resid_data = res0;  blk.col[0].resid_len = 4;
    blk.col[0].stats = (striq_col_stats_t){ 20.0, 22.0, 84.0, 4, 1 };
    blk.col[1].codec      = CODEC_RAW_STATS;
    blk.col[1].base_data  = base1; blk.col[1].base_len = 32;
    blk.col[1].resid_data = NULL;  blk.col[1].resid_len = 0;
    blk.col[1].stats = (striq_col_stats_t){ 40.0, 60.0, 200.0, 4, 0 };

    assert(fmt_writer_write_block(&w, &blk) == STRIQ_OK);
    assert(fmt_writer_write_footer(&w) == STRIQ_OK);

    /* Now read it back */
    mem_ctx_t rctx = { mem, sizeof(mem), wctx.size, 0 };
    static striq_fmt_reader_t r;
    assert(fmt_reader_open(&r, mem_read, mem_seek, &rctx, wctx.size) == STRIQ_OK);

    assert(r.num_blocks == 1);
    assert(r.num_cols   == 2);
    assert(r.block_index[0].num_rows == 4);
    assert(r.block_stats[0][0].num_segments == 1);
    assert(r.block_stats[0][1].num_segments == 0);

    printf("  [PASS] format_write_read_roundtrip (%zu bytes)\n", wctx.size);
}

int main(void)
{
    printf("=== test_format ===\n");
    test_write_read_roundtrip();
    printf("All format tests passed.\n");
    return 0;
}
