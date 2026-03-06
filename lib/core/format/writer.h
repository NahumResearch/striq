#ifndef STRIQ_WRITER_H
#define STRIQ_WRITER_H



#include "../types.h"

typedef struct {
    striq_write_fn write_fn;
    void          *write_ctx;

    striq_col_schema_t cols[STRIQ_MAX_COLS];
    uint32_t           num_cols;
    double             epsilon_b;
    double             col_epsilons[STRIQ_MAX_COLS];  

    striq_block_index_t block_index[STRIQ_MAX_BLOCKS];
    striq_col_stats_t   block_stats[STRIQ_MAX_BLOCKS][STRIQ_MAX_COLS];
    uint32_t            num_blocks;

    uint64_t bytes_written; 
    uint64_t total_rows;
    int64_t  ts_min, ts_max;
    uint32_t block_crc;      
    int      in_block;       
} striq_fmt_writer_t;

striq_status_t fmt_writer_init(
    striq_fmt_writer_t *w,
    const striq_col_schema_t *cols,
    uint32_t num_cols,
    double epsilon_b,
    striq_write_fn write_fn,
    void *write_ctx
);


striq_status_t fmt_writer_write_header(striq_fmt_writer_t *w);

/* Write one compressed block. */
typedef struct {
    int64_t  ts_first, ts_last;
    uint32_t num_rows;
    const uint8_t *ts_data;
    size_t         ts_len;
    struct {
        striq_codec_t  codec;
        const uint8_t *base_data;  
        size_t         base_len;
        const uint8_t *resid_data;  
        size_t         resid_len;
        striq_col_stats_t stats;
    } col[STRIQ_MAX_COLS];
} striq_block_data_t;

striq_status_t fmt_writer_write_block(
    striq_fmt_writer_t     *w,
    const striq_block_data_t *blk
);


striq_status_t fmt_writer_write_footer(striq_fmt_writer_t *w);

#endif 
