#ifndef STRIQ_DECODER_H
#define STRIQ_DECODER_H

#include "types.h"
#include "format/reader.h"

typedef struct {
    striq_fmt_reader_t fmt;
    double epsilon_b;
} striq_decoder_t;

striq_status_t decoder_init(
    striq_decoder_t *d,
    striq_read_fn    read_fn,
    striq_seek_fn    seek_fn,
    void            *io_ctx,
    uint64_t         file_size
);

/*
 * Decode all values for a given block and column into `out`.
 * `out` must hold at least block.num_rows doubles.
 */
striq_status_t decoder_read_column(
    striq_decoder_t *d,
    uint32_t         block_idx,
    uint32_t         col_idx,
    double          *out,
    size_t           out_cap
);

/*
 * Decode timestamps for a given block into `out`.
 * `out` must hold at least block.num_rows int64 values.
 */
striq_status_t decoder_read_timestamps(
    striq_decoder_t *d,
    uint32_t         block_idx,
    int64_t         *out,
    size_t           out_cap
);

#endif /* STRIQ_DECODER_H */
