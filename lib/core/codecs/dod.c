#include "dod.h"
#include "zigzag.h"
#include "varint.h"
#include <string.h>
#include <stdbool.h>

#define varint_encode varint_write_u64
#define varint_decode varint_read_u64

striq_status_t dod_encode(
    const int64_t *timestamps,
    size_t         n,
    uint8_t       *out_buf,
    size_t         out_cap,
    size_t        *out_len)
{
    if (!out_buf || !out_len) return STRIQ_ERR_PARAM;
    if (n == 0) { *out_len = 0; return STRIQ_OK; }
    if (!timestamps) return STRIQ_ERR_PARAM;
    if (out_cap < 8) return STRIQ_ERR_MEMORY;

    size_t pos = 0;

    uint64_t raw = (uint64_t)timestamps[0];
    for (int b = 0; b < 8; b++)
        out_buf[pos++] = (uint8_t)(raw >> (b * 8));

    if (n == 1) { *out_len = pos; return STRIQ_OK; }

    int64_t prev_delta = timestamps[1] - timestamps[0];
    size_t  written = varint_encode(zigzag_encode(prev_delta),
                                    out_buf + pos, out_cap - pos);
    if (written == 0) return STRIQ_ERR_MEMORY;
    pos += written;

    for (size_t i = 2; i < n; i++) {
        int64_t delta = timestamps[i] - timestamps[i - 1];
        int64_t dod   = delta - prev_delta;
        written = varint_encode(zigzag_encode(dod), out_buf + pos, out_cap - pos);
        if (written == 0) return STRIQ_ERR_MEMORY;
        pos += written;
        prev_delta = delta;
    }

    *out_len = pos;
    return STRIQ_OK;
}

striq_status_t dod_decode(
    const uint8_t *in_buf,
    size_t         in_len,
    size_t         n,
    int64_t       *out_timestamps)
{
    if (!in_buf || !out_timestamps) return STRIQ_ERR_PARAM;
    if (n == 0) return STRIQ_OK;
    if (in_len < 8) return STRIQ_ERR_FORMAT;

    size_t pos = 0;

    uint64_t raw = 0;
    for (int b = 0; b < 8; b++)
        raw |= (uint64_t)in_buf[pos++] << (b * 8);
    out_timestamps[0] = (int64_t)raw;

    if (n == 1) return STRIQ_OK;

    uint64_t zz;
    size_t consumed = varint_decode(in_buf + pos, in_len - pos, &zz);
    if (consumed == 0) return STRIQ_ERR_FORMAT;
    pos += consumed;
    int64_t prev_delta = zigzag_decode(zz);
    out_timestamps[1] = out_timestamps[0] + prev_delta;

    for (size_t i = 2; i < n; i++) {
        consumed = varint_decode(in_buf + pos, in_len - pos, &zz);
        if (consumed == 0) return STRIQ_ERR_FORMAT;
        pos += consumed;
        int64_t dod   = zigzag_decode(zz);
        int64_t delta = prev_delta + dod;
        out_timestamps[i] = out_timestamps[i - 1] + delta;
        prev_delta = delta;
    }

    return STRIQ_OK;
}

striq_status_t dod_encode_indexed(
    const int64_t          *timestamps,
    size_t                  n,
    uint8_t                *out_buf,
    size_t                  out_cap,
    size_t                 *out_len,
    striq_ts_index_entry_t *out_index,
    uint16_t               *out_count)
{
    if (!out_buf || !out_len || !out_index || !out_count) return STRIQ_ERR_PARAM;
    if (n == 0) { *out_len = 0; *out_count = 0; return STRIQ_OK; }
    if (!timestamps) return STRIQ_ERR_PARAM;
    if (out_cap < 8) return STRIQ_ERR_MEMORY;

    uint16_t idx_n = 0;
    size_t   pos   = 0;

#define MAYBE_INDEX(row_i) \
    do { \
        if ((uint32_t)(row_i) % STRIQ_TS_INDEX_STEP == 0 \
                && idx_n < STRIQ_TS_INDEX_MAX_ENTRIES) { \
            out_index[idx_n].ts          = timestamps[(row_i)];  \
            out_index[idx_n].row_offset  = (uint32_t)(row_i);    \
            out_index[idx_n].byte_offset = (uint32_t)pos;        \
            idx_n++;                                              \
        } \
    } while (0)

    MAYBE_INDEX(0);
    uint64_t raw = (uint64_t)timestamps[0];
    for (int b = 0; b < 8; b++)
        out_buf[pos++] = (uint8_t)(raw >> (b * 8));

    if (n == 1) { *out_len = pos; *out_count = idx_n; return STRIQ_OK; }

    MAYBE_INDEX(1);
    int64_t prev_delta = timestamps[1] - timestamps[0];
    size_t  written = varint_encode(zigzag_encode(prev_delta),
                                    out_buf + pos, out_cap - pos);
    if (written == 0) return STRIQ_ERR_MEMORY;
    pos += written;

    for (size_t i = 2; i < n; i++) {
        MAYBE_INDEX(i);
        int64_t delta = timestamps[i] - timestamps[i - 1];
        int64_t dod   = delta - prev_delta;
        written = varint_encode(zigzag_encode(dod), out_buf + pos, out_cap - pos);
        if (written == 0) return STRIQ_ERR_MEMORY;
        pos += written;
        prev_delta = delta;
    }

#undef MAYBE_INDEX

    *out_len   = pos;
    *out_count = idx_n;
    return STRIQ_OK;
}

striq_status_t dod_find_range(
    const striq_ts_index_entry_t *index,
    uint16_t                      index_count,
    uint32_t                      n_rows,
    int64_t                       ts_from,
    int64_t                       ts_to,
    uint32_t                     *out_start_row,
    uint32_t                     *out_end_row)
{
    if (!out_start_row || !out_end_row) return STRIQ_ERR_PARAM;

    uint32_t last = n_rows > 0 ? n_rows - 1 : 0;

    if (ts_from == 0 && ts_to == 0) {
        *out_start_row = 0;
        *out_end_row   = last;
        return STRIQ_OK;
    }

    /* Find start: last index entry with ts <= ts_from gives the nearest
     * row ≤ ts_from. We return that as start_row; the engine will scan
     * forward from there to find the exact first matching row. */
    uint32_t start = 0;
    if (ts_from != 0 && index_count > 0) {
        for (uint16_t i = 0; i < index_count; i++) {
            if (index[i].ts <= ts_from)
                start = index[i].row_offset;
            else
                break;
        }
    }

    /* Find end: last index entry with ts <= ts_to, then advance one full
     * step to ensure we include all rows up to ts_to. */
    uint32_t end_row = last;
    if (ts_to != 0 && index_count > 0) {
        for (uint16_t i = 0; i < index_count; i++) {
            if (index[i].ts <= ts_to) {
                uint32_t candidate = index[i].row_offset
                                   + (uint32_t)STRIQ_TS_INDEX_STEP - 1;
                if (candidate > last) candidate = last;
                end_row = candidate;
            }
        }
    }

    *out_start_row = start;
    *out_end_row   = end_row;
    return STRIQ_OK;
}

/*
 * Decode only the rows in [start_row, end_row] (inclusive).
 * Uses the index to start decoding from the nearest preceding entry,
 * limiting the work to at most STRIQ_TS_INDEX_STEP rows of warm-up.
 */
striq_status_t dod_decode_range(
    const uint8_t                *dod_stream,
    size_t                        stream_len,
    size_t                        n_total,
    const striq_ts_index_entry_t *index,
    uint16_t                      index_count,
    uint32_t                      start_row,
    uint32_t                      end_row,
    int64_t                      *out_timestamps)
{
    if (!dod_stream || !out_timestamps) return STRIQ_ERR_PARAM;
    if (n_total == 0) return STRIQ_ERR_PARAM;
    if (start_row > end_row || end_row >= (uint32_t)n_total) return STRIQ_ERR_PARAM;
    if (stream_len < 8) return STRIQ_ERR_FORMAT;

    uint32_t seek_row  = 0;
    size_t   seek_byte = 0;
    for (uint16_t i = 0; i < index_count; i++) {
        if (index[i].row_offset <= start_row) {
            seek_row  = index[i].row_offset;
            seek_byte = index[i].byte_offset;
        } else {
            break;
        }
    }

    size_t  pos        = seek_byte;
    int64_t ts_cur     = 0;
    int64_t prev_delta = 0;
    uint32_t cur_row   = seek_row;

    if (seek_row == 0) {
        /* Row 0 is always a raw int64 LE */
        uint64_t raw = 0;
        for (int b = 0; b < 8; b++)
            raw |= (uint64_t)dod_stream[pos++] << (b * 8);
        ts_cur = (int64_t)raw;
        prev_delta = 0;
    } else {
        /* Index entry for seek_row gives us ts directly */
        ts_cur = index[0].ts; /* fallback */
        for (uint16_t i = 0; i < index_count; i++) {
            if (index[i].row_offset == seek_row) {
                ts_cur = index[i].ts;
                break;
            }
        }
        /* prev_delta is unknown from the index alone; we need to recover it.
         * Decode from byte 0 up to seek_row to get prev_delta. */
        uint64_t raw = 0;
        for (int b = 0; b < 8; b++)
            raw |= (uint64_t)dod_stream[b] << (b * 8);
        int64_t ts_walk = (int64_t)raw;
        size_t  p       = 8;
        prev_delta      = 0;
        for (uint32_t r = 0; r < seek_row && p < stream_len; r++) {
            uint64_t zz;
            size_t c = varint_decode(dod_stream + p, stream_len - p, &zz);
            if (c == 0) return STRIQ_ERR_FORMAT;
            p += c;
            if (r == 0) {
                prev_delta = zigzag_decode(zz);
                ts_walk   += prev_delta;
            } else {
                prev_delta += zigzag_decode(zz);
                ts_walk    += prev_delta;
            }
        }
        /* pos was already set to seek_byte (the byte for row seek_row's DoD) */
        (void)ts_walk; /* ts_cur already set from index */
    }

    uint32_t out_i = 0;
    if (cur_row >= start_row && cur_row <= end_row)
        out_timestamps[out_i++] = ts_cur;

    while (cur_row < end_row && pos < stream_len) {
        uint64_t zz;
        size_t c = varint_decode(dod_stream + pos, stream_len - pos, &zz);
        if (c == 0) return STRIQ_ERR_FORMAT;
        pos += c;

        if (cur_row == 0) {
            /* first varint after raw t[0] is the first delta */
            prev_delta = zigzag_decode(zz);
        } else {
            prev_delta += zigzag_decode(zz);
        }
        ts_cur += prev_delta;
        cur_row++;

        if (cur_row >= start_row && cur_row <= end_row)
            out_timestamps[out_i++] = ts_cur;
    }

    return STRIQ_OK;
}
