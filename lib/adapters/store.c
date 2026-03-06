#include "store.h"
#include "../core/format/writer.h"
#include "../core/format/reader.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef struct {
    uint8_t *buf;
    size_t   cap;
    size_t   size;
    size_t   pos;
} store_mctx_t;

static striq_status_t smw(const uint8_t *d, size_t n, void *c)
{
    store_mctx_t *m = (store_mctx_t *)c;
    if (m->size + n > m->cap) return STRIQ_ERR_MEMORY;
    memcpy(m->buf + m->size, d, n);
    m->size += n;
    return STRIQ_OK;
}
static striq_status_t smr(uint8_t *b, size_t n, void *c)
{
    store_mctx_t *m = (store_mctx_t *)c;
    if (m->pos + n > m->size) return STRIQ_ERR_IO;
    memcpy(b, m->buf + m->pos, n);
    m->pos += n;
    return STRIQ_OK;
}
static striq_status_t sms(int64_t off, int wh, void *c)
{
    store_mctx_t *m = (store_mctx_t *)c;
    size_t np = (wh == 0) ? (size_t)off
              : (wh == 1) ? m->pos  + (size_t)off
              :              m->size + (size_t)off;
    if (np > m->size) return STRIQ_ERR_IO;
    m->pos = np;
    return STRIQ_OK;
}

typedef struct {
    striq_block_provider_t  base;     /* MUST be first */
    striq_file_provider_t  *cold;     /* NULL if no cold or not yet synced */
    striq_memstore_t       *warm;
    uint32_t                cold_cnt; /* snapshot of cold block count */
} store_multi_t;

static striq_status_t smp_get_block_index(
    void *ctx, uint32_t idx, striq_block_index_t *out)
{
    store_multi_t *mp = (store_multi_t *)ctx;
    if (idx < mp->cold_cnt && mp->cold)
        return mp->cold->base.get_block_index(mp->cold->base.ctx, idx, out);
    striq_block_provider_t *wp = memstore_provider(mp->warm);
    return wp->get_block_index(wp->ctx, idx - mp->cold_cnt, out);
}

static striq_status_t smp_get_block_data(
    void *ctx, uint32_t idx, const uint8_t **data, uint32_t *size)
{
    store_multi_t *mp = (store_multi_t *)ctx;
    if (idx < mp->cold_cnt && mp->cold)
        return mp->cold->base.get_block_data(mp->cold->base.ctx, idx, data, size);
    striq_block_provider_t *wp = memstore_provider(mp->warm);
    return wp->get_block_data(wp->ctx, idx - mp->cold_cnt, data, size);
}

static void smp_release_block(void *ctx, uint32_t idx, const uint8_t *data)
{
    store_multi_t *mp = (store_multi_t *)ctx;
    if (idx < mp->cold_cnt && mp->cold)
        mp->cold->base.release_block(mp->cold->base.ctx, idx, data);
}

static striq_status_t smp_get_col_stats(
    void *ctx, uint32_t idx, uint32_t col_idx, striq_col_stats_t *out)
{
    store_multi_t *mp = (store_multi_t *)ctx;
    if (idx < mp->cold_cnt && mp->cold)
        return mp->cold->base.get_col_stats(mp->cold->base.ctx, idx, col_idx, out);
    striq_block_provider_t *wp = memstore_provider(mp->warm);
    return wp->get_col_stats(wp->ctx, idx - mp->cold_cnt, col_idx, out);
}

struct striq_store {
    striq_col_schema_t  cols[STRIQ_MAX_COLS];
    uint32_t            num_cols;
    double              epsilon_b;

    int64_t  ts_buf[ENCODER_MAX_ROWS_PER_BLOCK];
    double   val_buf[ENCODER_MAX_ROWS_PER_BLOCK][STRIQ_MAX_COLS];
    uint32_t buf_rows;

    striq_memstore_t  *warm;

    char              *cold_path;
    FILE              *cold_file;
    striq_file_ctx_t   cold_io;
    striq_fmt_writer_t cold_writer;   
    bool               cold_open;    

    striq_fmt_reader_t   cold_reader;
    striq_file_provider_t cold_prov;
    bool                  cold_valid;

    store_multi_t        multi;
    striq_query_engine_t qe;
};

static void store_evict(
    const uint8_t *data, uint32_t size,
    const striq_block_index_t *idx,
    const striq_col_stats_t *col_stats, uint32_t num_cols,
    void *user)
{
    striq_store_t *s = (striq_store_t *)user;
    if (!s->cold_open || !s->cold_file) return;
    if (s->cold_writer.num_blocks >= STRIQ_MAX_BLOCKS) return;

  
    uint32_t bi = s->cold_writer.num_blocks;
    s->cold_writer.block_index[bi].file_offset = s->cold_writer.bytes_written;
    s->cold_writer.block_index[bi].block_size  = size;
    s->cold_writer.block_index[bi].num_rows    = idx->num_rows;
    s->cold_writer.block_index[bi].ts_first    = idx->ts_first;
    s->cold_writer.block_index[bi].ts_last     = idx->ts_last;

    uint32_t nc = (num_cols < STRIQ_MAX_COLS) ? num_cols : STRIQ_MAX_COLS;
    if (col_stats)
        memcpy(s->cold_writer.block_stats[bi], col_stats,
               nc * sizeof(striq_col_stats_t));

    s->cold_writer.num_blocks++;
    s->cold_writer.total_rows += idx->num_rows;
    s->cold_writer.bytes_written += size;
    if (idx->ts_first < s->cold_writer.ts_min) s->cold_writer.ts_min = idx->ts_first;
    if (idx->ts_last  > s->cold_writer.ts_max) s->cold_writer.ts_max = idx->ts_last;

    fwrite(data, 1, size, s->cold_file);
}

static void store_update_multi(striq_store_t *s)
{
    striq_block_provider_t *wp = memstore_provider(s->warm);
    uint32_t cold_cnt = s->cold_valid ? s->cold_prov.base.num_blocks : 0;
    uint32_t warm_cnt = wp->num_blocks;

    s->multi.cold     = s->cold_valid ? &s->cold_prov : NULL;
    s->multi.warm     = s->warm;
    s->multi.cold_cnt = cold_cnt;

    s->multi.base.ctx         = &s->multi;
    s->multi.base.num_blocks  = cold_cnt + warm_cnt;
    s->multi.base.num_cols    = s->num_cols;
    s->multi.base.epsilon_b   = s->epsilon_b;
    s->multi.base.total_rows  = (s->cold_valid ? s->cold_prov.base.total_rows : 0)
                               + wp->total_rows;
    s->multi.base.col_schemas = s->cols;
    s->multi.base.get_block_index = smp_get_block_index;
    s->multi.base.get_block_data  = smp_get_block_data;
    s->multi.base.release_block   = smp_release_block;
    s->multi.base.get_col_stats   = smp_get_col_stats;

    engine_init(&s->qe, &s->multi.base);
}

static striq_status_t store_flush_buffer(striq_store_t *s)
{
    if (s->buf_rows == 0) return STRIQ_OK;

    size_t tmp_cap = (size_t)s->buf_rows * s->num_cols * 64u + 65536u;
    uint8_t *tmp = malloc(tmp_cap);
    if (!tmp) return STRIQ_ERR_MEMORY;

    striq_encoder_t *enc = malloc(sizeof(*enc));
    if (!enc) { free(tmp); return STRIQ_ERR_MEMORY; }

    store_mctx_t wctx = { tmp, tmp_cap, 0, 0 };
    striq_opts_t enc_opts = striq_opts_make(); enc_opts.epsilon = s->epsilon_b;
    striq_status_t st = encoder_init(enc, s->cols, s->num_cols,
                                      &enc_opts, smw, &wctx);
    if (st != STRIQ_OK) { free(enc); free(tmp); return st; }

    for (uint32_t i = 0; i < s->buf_rows; i++) {
        st = encoder_add_row(enc, s->ts_buf[i], s->val_buf[i], s->num_cols);
        if (st != STRIQ_OK) { free(enc); free(tmp); return st; }
    }
    st = encoder_close(enc);
    free(enc);
    if (st != STRIQ_OK) { free(tmp); return st; }

    striq_fmt_reader_t *fmt = malloc(sizeof(*fmt));
    if (!fmt) { free(tmp); return STRIQ_ERR_MEMORY; }

    store_mctx_t rctx = { tmp, tmp_cap, wctx.size, 0 };
    st = fmt_reader_open(fmt, smr, sms, &rctx, wctx.size);
    if (st != STRIQ_OK) { free(fmt); free(tmp); return st; }

    for (uint32_t bi = 0; bi < fmt->num_blocks; bi++) {
        uint32_t bsz = fmt->block_index[bi].block_size;
        uint8_t *blk = malloc(bsz);
        if (!blk) { free(fmt); free(tmp); return STRIQ_ERR_MEMORY; }
        size_t rlen = 0;
        st = fmt_reader_read_block_raw(fmt, bi, blk, bsz, &rlen);
        if (st == STRIQ_OK)
            st = memstore_push_block(s->warm, blk, (uint32_t)rlen,
                                     &fmt->block_index[bi],
                                     fmt->block_stats[bi]);
        free(blk);
        if (st != STRIQ_OK) { free(fmt); free(tmp); return st; }
    }

    free(fmt);
    free(tmp);
    s->buf_rows = 0;
    return STRIQ_OK;
}

static striq_store_t *store_new(
    const striq_col_schema_t *cols, uint32_t num_cols, double epsilon_b,
    const char *cold_path, uint32_t warm_max_blocks, uint32_t warm_max_mb)
{
    striq_store_t *s = calloc(1, sizeof(*s));
    if (!s) return NULL;

    memcpy(s->cols, cols, num_cols * sizeof(striq_col_schema_t));
    s->num_cols  = num_cols;
    s->epsilon_b = epsilon_b;

    if (cold_path) {
        size_t len = strlen(cold_path) + 1;
        s->cold_path = (char *)malloc(len);
        if (s->cold_path) memcpy(s->cold_path, cold_path, len);
        if (!s->cold_path) { free(s); return NULL; }
    }

    striq_memstore_opts_t mopts = {
        .epsilon_b       = epsilon_b,
        .max_blocks      = warm_max_blocks ? warm_max_blocks : 256,
        .max_memory_mb   = warm_max_mb     ? warm_max_mb     : 16,
        .eviction_cb     = cold_path ? store_evict : NULL,
        .eviction_ctx    = s
    };
    s->warm = memstore_create(cols, num_cols, &mopts);
    if (!s->warm) { free(s->cold_path); free(s); return NULL; }

    return s;
}

striq_store_t *striq_store_create(
    const char * const    *col_names,
    const striq_coltype_t *col_types,
    uint32_t               num_cols,
    const striq_store_opts_t *opts)
{
    if (!col_names || !col_types || num_cols == 0 || num_cols > STRIQ_MAX_COLS)
        return NULL;

    striq_col_schema_t cols[STRIQ_MAX_COLS];
    for (uint32_t i = 0; i < num_cols; i++) {
        strncpy(cols[i].name, col_names[i], 63);
        cols[i].name[63] = '\0';
        cols[i].type  = col_types[i];
        cols[i].codec = CODEC_PLA_LINEAR;
    }

    double       eps  = opts ? opts->epsilon_b        : 0.0;
    const char  *cp   = opts ? opts->cold_path        : NULL;
    uint32_t     wmb  = opts ? opts->warm_max_blocks   : 0;
    uint32_t     wmm  = opts ? opts->warm_max_memory_mb: 0;

    striq_store_t *s = store_new(cols, num_cols, eps, cp, wmb, wmm);
    if (!s) return NULL;

    if (cp) {
        s->cold_file = fopen(cp, "wb");
        if (!s->cold_file) { striq_store_close(s); return NULL; }
        s->cold_io.fp = s->cold_file;

        fmt_writer_init(&s->cold_writer, cols, num_cols, eps,
                        file_io_write, &s->cold_io);
        if (fmt_writer_write_header(&s->cold_writer) != STRIQ_OK) {
            striq_store_close(s); return NULL;
        }
        s->cold_open = true;
    }

    store_update_multi(s);
    return s;
}

striq_store_t *striq_store_open(
    const char               *cold_path,
    const striq_store_opts_t *opts)
{
    if (!cold_path) return NULL;

    FILE *fp = fopen(cold_path, "rb");
    if (!fp) return NULL;

    striq_file_ctx_t fio = { fp };
    uint64_t fsz = file_io_size(fp);
    striq_fmt_reader_t *fmt = malloc(sizeof(*fmt));
    if (!fmt) { fclose(fp); return NULL; }
    if (fmt_reader_open(fmt, file_io_read, file_io_seek, &fio, fsz) != STRIQ_OK) {
        free(fmt); fclose(fp); return NULL;
    }
    fclose(fp);

    uint32_t wmb = opts ? opts->warm_max_blocks    : 0;
    uint32_t wmm = opts ? opts->warm_max_memory_mb : 0;
    striq_store_t *s = store_new(fmt->cols, fmt->num_cols, fmt->epsilon_b,
                                  cold_path, wmb, wmm);
    free(fmt);
    if (!s) return NULL;

    s->cold_file = fopen(cold_path, "rb");
    if (!s->cold_file) { striq_store_close(s); return NULL; }
    s->cold_io.fp = s->cold_file;

    if (fmt_reader_open(&s->cold_reader, file_io_read, file_io_seek,
                        &s->cold_io, fsz) == STRIQ_OK) {
        file_provider_init(&s->cold_prov, &s->cold_reader);
        s->cold_valid = true;
    }

    store_update_multi(s);
    return s;
}

striq_status_t striq_store_push(
    striq_store_t *s,
    int64_t        timestamp_ns,
    const double  *values,
    size_t         num_values)
{
    if (!s || !values) return STRIQ_ERR_PARAM;
    if (num_values != s->num_cols) return STRIQ_ERR_PARAM;

    if (s->buf_rows >= ENCODER_MAX_ROWS_PER_BLOCK) {
        STRIQ_TRY(store_flush_buffer(s));
        store_update_multi(s);
    }

    s->ts_buf[s->buf_rows] = timestamp_ns;
    for (uint32_t c = 0; c < s->num_cols; c++)
        s->val_buf[s->buf_rows][c] = values[c];
    s->buf_rows++;
    return STRIQ_OK;
}

static striq_status_t flush_and_update(striq_store_t *s)
{
    STRIQ_TRY(store_flush_buffer(s));
    store_update_multi(s);
    return STRIQ_OK;
}

static striq_status_t to_result(const striq_query_result_t *qr, striq_result_t *out)
{
    out->value         = qr->value;
    out->error_bound   = qr->error_bound;
    out->rows_scanned  = qr->rows_scanned;
    out->pct_data_read = qr->pct_data_read;
    return STRIQ_OK;
}

striq_status_t striq_store_query_mean(
    striq_store_t *s, const char *col,
    int64_t ts_from, int64_t ts_to, striq_result_t *out)
{
    if (!s || !out) return STRIQ_ERR_PARAM;
    STRIQ_TRY(flush_and_update(s));
    striq_query_result_t qr = {0};
    STRIQ_TRY(engine_query_mean(&s->qe, col, ts_from, ts_to, &qr));
    return to_result(&qr, out);
}

striq_status_t striq_store_query_count(
    striq_store_t *s, int64_t ts_from, int64_t ts_to, uint64_t *out)
{
    if (!s || !out) return STRIQ_ERR_PARAM;
    STRIQ_TRY(flush_and_update(s));
    return engine_query_count(&s->qe, ts_from, ts_to, out);
}

striq_status_t striq_store_query_min(
    striq_store_t *s, const char *col,
    int64_t ts_from, int64_t ts_to, striq_result_t *out)
{
    if (!s || !out) return STRIQ_ERR_PARAM;
    STRIQ_TRY(flush_and_update(s));
    striq_query_result_t qr = {0};
    STRIQ_TRY(engine_query_min(&s->qe, col, ts_from, ts_to, &qr));
    return to_result(&qr, out);
}

striq_status_t striq_store_query_max(
    striq_store_t *s, const char *col,
    int64_t ts_from, int64_t ts_to, striq_result_t *out)
{
    if (!s || !out) return STRIQ_ERR_PARAM;
    STRIQ_TRY(flush_and_update(s));
    striq_query_result_t qr = {0};
    STRIQ_TRY(engine_query_max(&s->qe, col, ts_from, ts_to, &qr));
    return to_result(&qr, out);
}

striq_status_t striq_store_sync(striq_store_t *s)
{
    if (!s) return STRIQ_ERR_PARAM;
    STRIQ_TRY(store_flush_buffer(s));

    if (!s->cold_open || !s->cold_file || !s->cold_path) {
        store_update_multi(s);
        return STRIQ_OK;
    }

    /* Write footer to cold file at current position (always, even 0 blocks) */
    fflush(s->cold_file);
    fmt_writer_write_footer(&s->cold_writer);
    fflush(s->cold_file);
    fclose(s->cold_file);
    s->cold_file  = NULL;
    s->cold_open  = false;

    FILE *rfp = fopen(s->cold_path, "rb");
    if (rfp) {
        s->cold_file = rfp;
        s->cold_io.fp = rfp;
        uint64_t fsz  = file_io_size(rfp);
        if (fmt_reader_open(&s->cold_reader, file_io_read, file_io_seek,
                            &s->cold_io, fsz) == STRIQ_OK) {
            file_provider_init(&s->cold_prov, &s->cold_reader);
            s->cold_valid = true;
        }
    }

    store_update_multi(s);
    return STRIQ_OK;
}

striq_status_t striq_store_close(striq_store_t *s)
{
    if (!s) return STRIQ_ERR_PARAM;
    striq_store_sync(s);
    if (s->cold_file) { fclose(s->cold_file); s->cold_file = NULL; }
    if (s->cold_path) { free(s->cold_path); s->cold_path = NULL; }
    memstore_destroy(s->warm);
    free(s);
    return STRIQ_OK;
}
