#include "../../include/striq.h"
#include "file_io.h"
#include "file_provider.h"
#include "mmap_provider.h"
#include "../core/encoder.h"
#include "../core/decoder.h"
#include "../core/query/engine.h"
#include "../core/format/reader.h"
#include "../core/codecs/crc32.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

const char *striq_status_str(striq_status_t s)
{
    switch (s) {
        case STRIQ_OK:             return "OK";
        case STRIQ_ERR_IO:         return "I/O error";
        case STRIQ_ERR_FORMAT:     return "format error";
        case STRIQ_ERR_MEMORY:     return "out of memory";
        case STRIQ_ERR_PARAM:      return "invalid parameter";
        case STRIQ_ERR_CODEC:      return "codec error";
        case STRIQ_ERR_QUERY:      return "query error";
        case STRIQ_ERR_NOTFOUND:   return "not found";
        case STRIQ_ERR_NOTIMPL:    return "not implemented";
        case STRIQ_ERR_CORRUPT:    return "data corruption (CRC mismatch)";
        case STRIQ_ERR_VERSION:    return "unsupported format version";
        default:                   return "unknown error";
    }
}

struct striq_writer {
    striq_encoder_t  enc;
    striq_file_ctx_t file_ctx;
    FILE            *fp;
    uint32_t         num_cols;
};

striq_writer_t *striq_writer_open(
    const char            *path,
    const char * const    *col_names,
    const striq_coltype_t *col_types,
    size_t                 num_cols,
    const striq_opts_t    *opts)
{
    if (!path || !col_names || !col_types || num_cols == 0 || num_cols > STRIQ_MAX_COLS)
        return NULL;

    striq_writer_t *w = calloc(1, sizeof(*w));
    if (!w) return NULL;

    w->fp = fopen(path, "wb");
    if (!w->fp) { free(w); return NULL; }
    w->file_ctx.fp = w->fp;
    w->num_cols    = (uint32_t)num_cols;

    striq_col_schema_t cols[STRIQ_MAX_COLS];
    for (size_t i = 0; i < num_cols; i++) {
        strncpy(cols[i].name, col_names[i], 63);
        cols[i].name[63] = '\0';
        cols[i].type      = (striq_coltype_t)col_types[i];
        cols[i].codec     = CODEC_PLA_LINEAR;
        cols[i].epsilon_b = opts ? opts->col_epsilon[i] : 0.0;
    }

    striq_status_t s = encoder_init(
        &w->enc, cols, (uint32_t)num_cols, opts,
        file_io_write, &w->file_ctx);
    if (s != STRIQ_OK) { fclose(w->fp); free(w); return NULL; }

    return w;
}

striq_status_t striq_writer_add_row(
    striq_writer_t *w,
    int64_t         timestamp_ns,
    const double   *values,
    size_t          num_values)
{
    if (!w) return STRIQ_ERR_PARAM;
    return encoder_add_row(&w->enc, timestamp_ns, values, num_values);
}

striq_status_t striq_writer_close(striq_writer_t *w)
{
    if (!w) return STRIQ_ERR_PARAM;
    striq_status_t s = encoder_close(&w->enc);
    fclose(w->fp);
    free(w);
    return s;
}


struct striq_reader {
    striq_query_engine_t  qe;
    int                   use_mmap;

    striq_mmap_provider_t mmap_prov;
    striq_fmt_reader_t    mmap_fmt;

    striq_decoder_t       dec;
    striq_file_provider_t file_prov;
    striq_file_ctx_t      file_ctx;
    FILE                 *fp_file;
};

striq_reader_t *striq_reader_open(const char *path)
{
    if (!path) return NULL;
    striq_reader_t *r = calloc(1, sizeof(*r));
    if (!r) return NULL;

    striq_status_t s = mmap_provider_open(&r->mmap_prov, &r->mmap_fmt, path);
    if (s == STRIQ_OK) {
        r->use_mmap = 1;
        engine_init(&r->qe, &r->mmap_prov.base);
        return r;
    }

    r->fp_file = fopen(path, "rb");
    if (!r->fp_file) { free(r); return NULL; }
    r->file_ctx.fp = r->fp_file;

    uint64_t fsz = file_io_size(r->fp_file);
    s = decoder_init(&r->dec, file_io_read, file_io_seek, &r->file_ctx, fsz);
    if (s != STRIQ_OK) { fclose(r->fp_file); free(r); return NULL; }

    file_provider_init(&r->file_prov, &r->dec.fmt);
    engine_init(&r->qe, &r->file_prov.base);
    return r;
}

striq_status_t striq_reader_close(striq_reader_t *r)
{
    if (!r) return STRIQ_ERR_PARAM;
    if (r->use_mmap) {
        mmap_provider_close(&r->mmap_prov);
    } else {
        fclose(r->fp_file);
    }
    free(r);
    return STRIQ_OK;
}

striq_status_t striq_query_mean(
    striq_reader_t *r,
    const char     *col_name,
    int64_t         ts_from,
    int64_t         ts_to,
    striq_result_t *out)
{
    if (!r || !out) return STRIQ_ERR_PARAM;
    return engine_query_mean(&r->qe, col_name, ts_from, ts_to, out);
}

striq_status_t striq_query_count(
    striq_reader_t *r,
    int64_t         ts_from,
    int64_t         ts_to,
    uint64_t       *out)
{
    if (!r || !out) return STRIQ_ERR_PARAM;
    return engine_query_count(&r->qe, ts_from, ts_to, out);
}

striq_status_t striq_query_mean_where(
    striq_reader_t *r,
    const char     *col_name,
    int64_t         ts_from,
    int64_t         ts_to,
    double          threshold,
    striq_cmp_t     cmp,
    striq_result_t *out)
{
    if (!r || !out) return STRIQ_ERR_PARAM;
    return engine_query_mean_where(&r->qe, col_name, ts_from, ts_to,
                                   threshold, cmp, out);
}

striq_status_t striq_query_sum(
    striq_reader_t *r,
    const char     *col_name,
    int64_t         ts_from,
    int64_t         ts_to,
    striq_result_t *out)
{
    if (!r || !out) return STRIQ_ERR_PARAM;
    striq_result_t res = {0};
    STRIQ_TRY(engine_query_mean(&r->qe, col_name, ts_from, ts_to, &res));
    out->value         = res.value * (double)res.rows_scanned;
    out->error_bound   = res.error_bound * (double)res.rows_scanned;
    out->rows_scanned  = res.rows_scanned;
    out->pct_data_read = res.pct_data_read;
    out->pct_algebraic = res.pct_algebraic;
    return STRIQ_OK;
}

striq_status_t striq_query_min(
    striq_reader_t *r,
    const char     *col_name,
    int64_t         ts_from,
    int64_t         ts_to,
    striq_result_t *out)
{
    if (!r || !out) return STRIQ_ERR_PARAM;
    return engine_query_min(&r->qe, col_name, ts_from, ts_to, out);
}

striq_status_t striq_query_max(
    striq_reader_t *r,
    const char     *col_name,
    int64_t         ts_from,
    int64_t         ts_to,
    striq_result_t *out)
{
    if (!r || !out) return STRIQ_ERR_PARAM;
    return engine_query_max(&r->qe, col_name, ts_from, ts_to, out);
}

striq_status_t striq_query_variance(
    striq_reader_t *r,
    const char     *col_name,
    int64_t         ts_from,
    int64_t         ts_to,
    striq_result_t *out)
{
    if (!r || !out) return STRIQ_ERR_PARAM;
    return engine_query_variance(&r->qe, col_name, ts_from, ts_to, out);
}

static int resolve_col(striq_block_provider_t *p, const char *name)
{
    for (uint32_t c = 0; c < p->num_cols; c++)
        if (strncmp(p->col_schemas[c].name, name, 63) == 0) return (int)c;
    return -1;
}

static striq_status_t resolve_cols(
    striq_block_provider_t *p,
    const char * const     *col_names,
    uint32_t                num_cols,
    uint32_t               *indices,
    uint32_t               *out_n)
{
    if (!col_names) {
        for (uint32_t i = 0; i < p->num_cols; i++) indices[i] = i;
        *out_n = p->num_cols;
        return STRIQ_OK;
    }
    for (uint32_t i = 0; i < num_cols; i++) {
        int ci = resolve_col(p, col_names[i]);
        if (ci < 0) return STRIQ_ERR_NOTFOUND;
        indices[i] = (uint32_t)ci;
    }
    *out_n = num_cols;
    return STRIQ_OK;
}

striq_status_t striq_query_value_at(
    striq_reader_t     *r,
    const char * const *col_names,
    uint32_t            num_cols,
    int64_t             timestamp_ns,
    double             *out_values,
    double             *out_errors,
    uint32_t           *out_num_cols)
{
    if (!r || !out_values) return STRIQ_ERR_PARAM;
    uint32_t indices[STRIQ_MAX_COLS], n = 0;
    STRIQ_TRY(resolve_cols(r->qe.provider, col_names, num_cols, indices, &n));
    if (out_num_cols) *out_num_cols = n;
    return engine_query_value_at(&r->qe, indices, n, timestamp_ns,
                                 out_values, out_errors);
}

striq_status_t striq_query_scan(
    striq_reader_t     *r,
    const char * const *col_names,
    uint32_t            num_cols,
    int64_t             ts_from,
    int64_t             ts_to,
    double             *out_values,
    int64_t            *out_timestamps,
    uint32_t            max_rows,
    uint32_t           *out_num_rows,
    uint32_t           *out_num_cols)
{
    if (!r || !out_values || max_rows == 0) return STRIQ_ERR_PARAM;
    uint32_t indices[STRIQ_MAX_COLS], n = 0;
    STRIQ_TRY(resolve_cols(r->qe.provider, col_names, num_cols, indices, &n));
    if (out_num_cols) *out_num_cols = n;
    return engine_query_scan(&r->qe, indices, n, ts_from, ts_to,
                             out_values, out_timestamps, max_rows, out_num_rows);
}

striq_status_t striq_query_downsample(
    striq_reader_t *r,
    const char     *col_name,
    int64_t         ts_from,
    int64_t         ts_to,
    uint32_t        n_points,
    double         *out_values,
    int64_t        *out_ts)
{
    if (!r || !col_name || !out_values || n_points == 0) return STRIQ_ERR_PARAM;
    return engine_query_downsample(&r->qe, col_name, ts_from, ts_to,
                                   n_points, out_values, out_ts);
}

striq_status_t striq_verify(
    const char *path,
    uint32_t   *num_blocks_checked,
    uint32_t   *num_blocks_corrupt)
{
    if (!path) return STRIQ_ERR_PARAM;

    FILE *fp = fopen(path, "rb");
    if (!fp) return STRIQ_ERR_IO;

    striq_file_ctx_t fc = { fp };
    uint64_t fsz = file_io_size(fp);
    striq_fmt_reader_t *fmt = calloc(1, sizeof(*fmt));
    if (!fmt) { fclose(fp); return STRIQ_ERR_MEMORY; }
    striq_status_t s = fmt_reader_open(fmt, file_io_read, file_io_seek, &fc, fsz);
    if (s != STRIQ_OK) { free(fmt); fclose(fp); return s; }

    uint32_t checked = 0, corrupt = 0;
    for (uint32_t bi = 0; bi < fmt->num_blocks; bi++) {
        uint32_t bsz = fmt->block_index[bi].block_size;
        uint8_t *buf = malloc(bsz);
        if (!buf) { free(fmt); fclose(fp); return STRIQ_ERR_MEMORY; }
        size_t got = 0;
        striq_status_t rs = fmt_reader_read_block_raw(fmt, bi, buf, bsz, &got);
        if (rs == STRIQ_OK) {
            checked++;
        } else if (rs == STRIQ_ERR_CORRUPT) {
            checked++;
            corrupt++;
        }
        free(buf);
    }

    free(fmt);
    fclose(fp);
    if (num_blocks_checked) *num_blocks_checked = checked;
    if (num_blocks_corrupt)  *num_blocks_corrupt  = corrupt;
    return STRIQ_OK;
}

striq_status_t striq_inspect(const char *path, striq_file_info_t *out)
{
    if (!path || !out) return STRIQ_ERR_PARAM;
    striq_reader_t *r = striq_reader_open(path);
    if (!r) return STRIQ_ERR_IO;

    const striq_fmt_reader_t *fmt = r->use_mmap ? &r->mmap_fmt : &r->dec.fmt;

    out->num_blocks = fmt->num_blocks;
    out->total_rows = fmt->total_rows;
    out->num_cols   = fmt->num_cols;
    out->epsilon_b  = fmt->epsilon_b;
    out->ts_min     = fmt->ts_min;
    out->ts_max     = fmt->ts_max;

    uint32_t nc = fmt->num_cols;
    for (uint32_t c = 0; c < nc && c < STRIQ_MAX_COLS; c++) {
        strncpy(out->col_names[c], fmt->cols[c].name, 63);
        out->col_names[c][63] = '\0';
        out->col_codec[c]     = (uint8_t)fmt->cols[c].codec;
        uint64_t total = 0;
        for (uint32_t bi = 0; bi < fmt->num_blocks; bi++) {
            const striq_col_stats_t *st = &fmt->block_stats[bi][c];
            uint64_t seg = st->num_segments;
            switch ((striq_codec_t)fmt->cols[c].codec) {
                case CODEC_PLA_LINEAR: total += 48 + seg * 18; break;
                case CODEC_PLA_CHEB:  total += 48 + seg * 34; break;
                case CODEC_RAW_STATS: total += 48 + st->count * 8; break;
                default:              total += 48 + st->count * 4; break;
            }
        }
        out->col_compressed_bytes[c] = total;
    }

    return striq_reader_close(r);
}
