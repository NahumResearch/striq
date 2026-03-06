#ifndef STRIQ_ENCODER_H
#define STRIQ_ENCODER_H

/*
 * Orchestrates: buffer rows → route → compress → write block.
 *
 * Data flow:
 *   encoder_add_row() → accumulate in ring buffer
 *   When buffer full (block_size bytes) → flush_block()
 *     → router: pick codec per column
 *     → dod:    encode timestamps
 *     → pla/lz4: encode values
 *     → writer: serialise block
 *   encoder_flush() → write remaining rows + footer
 */

#include "types.h"
#include "arena.h"
#include "format/writer.h"

#define ENCODER_MAX_ROWS_PER_BLOCK 4096u

typedef struct {
    striq_fmt_writer_t fmt;

    /* Row buffer — column-major for cache-friendly per-column access */
    int64_t  ts_buf[ENCODER_MAX_ROWS_PER_BLOCK];
    double   val_buf[STRIQ_MAX_COLS][ENCODER_MAX_ROWS_PER_BLOCK];
    uint32_t buf_rows;

    double   epsilon_b;
    double   col_epsilons[STRIQ_MAX_COLS];  /* per-column overrides; 0 = use epsilon_b */
    uint64_t col_skip;                      /* bitmask: bit i set → force RAW_STATS for col i */

    /* Codec decision per column (cached after first block) */
    striq_codec_t col_codec[STRIQ_MAX_COLS];
    bool          codec_decided;

    /* Arena for per-block scratch memory */
    uint8_t  arena_mem[2 * 1024 * 1024];
    striq_arena_t arena;
} striq_encoder_t;

striq_status_t encoder_init(
    striq_encoder_t          *e,
    const striq_col_schema_t *cols,
    uint32_t                  num_cols,
    const striq_opts_t       *opts,  /* NULL = STRIQ_DEFAULTS */
    striq_write_fn            write_fn,
    void                     *write_ctx
);

striq_status_t encoder_add_row(
    striq_encoder_t *e,
    int64_t          timestamp_ns,
    const double    *values,
    size_t           num_values
);

/* Flush remaining rows and write footer. */
striq_status_t encoder_close(striq_encoder_t *e);

#endif /* STRIQ_ENCODER_H */
