#ifndef STRIQ_DOD_H
#define STRIQ_DOD_H

/*
 * Delta-of-Delta codec for timestamps.
 *
 * Encoding:
 *   t[0]          → raw int64 LE (8 bytes)
 *   t[1]-t[0]     → varint(zigzag(delta))
 *   i >= 2: dod   → varint(zigzag((t[i]-t[i-1]) - (t[i-1]-t[i-2])))
 *
 * Regular intervals produce dod=0 → 1 byte/timestamp after the first two.
 */

#include "../types.h"

/*
 * Encode `n` timestamps into `out_buf`.
 * `out_buf` must be at least n*10 bytes (worst case 10B/value).
 * `out_len` receives the actual number of bytes written.
 */
striq_status_t dod_encode(
    const int64_t *timestamps,
    size_t         n,
    uint8_t       *out_buf,
    size_t         out_cap,
    size_t        *out_len
);

/*
 * Decode `n` timestamps from `in_buf` (in_len bytes).
 * `out_timestamps` must hold at least n int64 values.
 */
striq_status_t dod_decode(
    const uint8_t *in_buf,
    size_t         in_len,
    size_t         n,
    int64_t       *out_timestamps
);

/*
 * Encode `n` timestamps and produce a sparse index for O(1) seek.
 * One index entry is written every STRIQ_TS_INDEX_STEP rows.
 *
 * out_buf    : DoD-encoded stream (same format as dod_encode).
 * out_len    : bytes written to out_buf.
 * out_index  : caller provides array of at least STRIQ_TS_INDEX_MAX_ENTRIES.
 * out_count  : number of entries filled in out_index.
 */
striq_status_t dod_encode_indexed(
    const int64_t          *timestamps,
    size_t                  n,
    uint8_t                *out_buf,
    size_t                  out_cap,
    size_t                 *out_len,
    striq_ts_index_entry_t *out_index,
    uint16_t               *out_count
);

/*
 * Binary-search `index` to find the inclusive row range [*out_start, *out_end]
 * that covers all timestamps in [ts_from, ts_to].
 * When ts_from==ts_to==0 the full range [0, n_rows-1] is returned.
 * n_rows is the total row count in the block.
 */
striq_status_t dod_find_range(
    const striq_ts_index_entry_t *index,
    uint16_t                      index_count,
    uint32_t                      n_rows,
    int64_t                       ts_from,
    int64_t                       ts_to,
    uint32_t                     *out_start_row,
    uint32_t                     *out_end_row
);

/*
 * Decode only rows [start_row, end_row] (inclusive) from a DoD stream,
 * using the index to skip directly to the nearest preceding entry.
 * out_timestamps must hold (end_row - start_row + 1) values.
 */
striq_status_t dod_decode_range(
    const uint8_t                *dod_stream,
    size_t                        stream_len,
    size_t                        n_total,     /* total rows encoded in stream */
    const striq_ts_index_entry_t *index,
    uint16_t                      index_count,
    uint32_t                      start_row,
    uint32_t                      end_row,
    int64_t                      *out_timestamps
);

#endif /* STRIQ_DOD_H */
