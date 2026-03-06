#ifndef STRIQ_READER_H
#define STRIQ_READER_H

/*
 * Footer-first reader for .striq files.
 * Reads the last 8 bytes to find the footer offset, then reads
 * block index + stats without touching block data.
 */

#include "../types.h"

typedef struct {
    striq_read_fn read_fn;
    striq_seek_fn seek_fn;
    void         *io_ctx;

    /* File metadata (from header) */
    uint8_t  version;   /* 1 = Phase 0-2, 2 = Phase 3+ (CRC32) */
    uint32_t num_cols;
    uint32_t num_blocks;
    uint64_t total_rows;
    double   epsilon_b;
    int64_t  ts_min, ts_max;

    striq_col_schema_t  cols[STRIQ_MAX_COLS];
    striq_block_index_t block_index[STRIQ_MAX_BLOCKS];
    striq_col_stats_t   block_stats[STRIQ_MAX_BLOCKS][STRIQ_MAX_COLS];

    uint64_t file_size;
} striq_fmt_reader_t;

/*
 * Open a .striq file: reads header, footer, and block index into `r`.
 * Does NOT read block data.
 */
striq_status_t fmt_reader_open(
    striq_fmt_reader_t *r,
    striq_read_fn       read_fn,
    striq_seek_fn       seek_fn,
    void               *io_ctx,
    uint64_t            file_size
);

/*
 * Read one block's raw data into caller-provided buffers.
 * block_idx must be < r->num_blocks.
 */
striq_status_t fmt_reader_read_block_raw(
    striq_fmt_reader_t *r,
    uint32_t            block_idx,
    uint8_t            *buf,
    size_t              buf_cap,
    size_t             *out_len
);

#endif /* STRIQ_READER_H */
