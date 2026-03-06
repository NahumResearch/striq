#include "partition.h"
#include "file_io.h"
#include "../core/format/reader.h"
#include "../core/format/writer.h"
#include "../core/decoder.h"
#include "../core/encoder.h"
#include "../core/query/engine.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

#ifdef _WIN32
#  include <direct.h>
#  include <sys/types.h>
#  include <sys/stat.h>
#  define striq_mkdir(p) _mkdir(p)
#  ifndef S_ISDIR
#    define S_ISDIR(m) (((m) & _S_IFMT) == _S_IFDIR)
#  endif
#else
#  include <sys/stat.h>
#  define striq_mkdir(p) mkdir(p, 0755)
#endif

static bool entry_overlaps(const striq_manifest_entry_t *e,
                            int64_t ts_from, int64_t ts_to);

struct striq_partition {
    striq_manifest_t       manifest;
    striq_partition_opts_t opts;

    striq_store_t         *active;
    char                   active_path[780];
    int64_t                active_period_key;
    uint64_t               active_rows;
    uint32_t               part_seq;   
};

static int64_t period_key(int64_t ts_ns, striq_period_t period)
{
    int64_t ts_s = ts_ns / 1000000000LL;
    switch (period) {
        case PERIOD_HOUR:  return ts_s / 3600LL;
        case PERIOD_DAY:   return ts_s / 86400LL;
        case PERIOD_MONTH: return ts_s / (30LL * 86400LL);
        default:           return ts_s / 86400LL;
    }
}

static void build_part_path(
    char *out, size_t cap,
    const char *dir, striq_period_t period, int64_t pkey, uint32_t seq)
{
    const char *tag;
    switch (period) {
        case PERIOD_HOUR:  tag = "h"; break;
        case PERIOD_DAY:   tag = "d"; break;
        case PERIOD_MONTH: tag = "m"; break;
        default:           tag = "d"; break;
    }
    snprintf(out, cap, "%s/part_%s_%lld_%u.striq", dir, tag, (long long)pkey, seq);
}

static striq_status_t ensure_dir(const char *path)
{
    struct stat st;
    if (stat(path, &st) == 0) {
        if (S_ISDIR(st.st_mode)) return STRIQ_OK;
        return STRIQ_ERR_IO;
    }
    if (striq_mkdir(path) != 0 && errno != EEXIST) return STRIQ_ERR_IO;
    return STRIQ_OK;
}

static striq_status_t finalize_active(striq_partition_t *p)
{
    if (!p->active) return STRIQ_OK;

    striq_status_t rc = striq_store_close(p->active);
    p->active = NULL;
    if (rc != STRIQ_OK) return rc;

    struct stat st;
    uint64_t file_sz = 0;
    if (stat(p->active_path, &st) == 0)
        file_sz = (uint64_t)st.st_size;

    striq_manifest_entry_t entry;
    memset(&entry, 0, sizeof(entry));

    const char *bname = strrchr(p->active_path, '/');
    bname = bname ? bname + 1 : p->active_path;
    size_t blen = strlen(bname);
    if (blen >= MANIFEST_PATH_MAX) blen = MANIFEST_PATH_MAX - 1;
    memcpy(entry.path, bname, blen);
    entry.file_size = file_sz;
    entry.num_rows  = p->active_rows;

    FILE *fp = fopen(p->active_path, "rb");
    if (fp) {
        striq_file_ctx_t fctx;
        fctx.fp = fp;
        uint64_t fsz = file_io_size(fp);

        striq_fmt_reader_t *rdr = malloc(sizeof(*rdr));
        if (rdr && fmt_reader_open(rdr, file_io_read, file_io_seek, &fctx, fsz) == STRIQ_OK) {
            entry.ts_first = rdr->ts_min;
            entry.ts_last  = rdr->ts_max;

            uint32_t nc = rdr->num_cols < (uint32_t)STRIQ_MAX_COLS
                        ? rdr->num_cols : (uint32_t)STRIQ_MAX_COLS;
            bool first_block = true;
            for (uint32_t bi = 0; bi < rdr->num_blocks; bi++) {
                for (uint32_t ci = 0; ci < nc; ci++) {
                    const striq_col_stats_t *s = &rdr->block_stats[bi][ci];
                    if (first_block) {
                        entry.col_stats[ci] = *s;
                    } else {
                        if (s->min < entry.col_stats[ci].min)
                            entry.col_stats[ci].min = s->min;
                        if (s->max > entry.col_stats[ci].max)
                            entry.col_stats[ci].max = s->max;
                        entry.col_stats[ci].sum   += s->sum;
                        entry.col_stats[ci].count += s->count;
                    }
                }
                first_block = false;
            }
        }
        free(rdr);
        fclose(fp);
    }

    manifest_upsert(&p->manifest, &entry);
    manifest_save(&p->manifest);

    p->active_rows       = 0;
    p->active_path[0]    = '\0';
    p->active_period_key = INT64_MIN;
    return STRIQ_OK;
}

static striq_status_t open_active(striq_partition_t *p, int64_t pkey)
{
    build_part_path(p->active_path, sizeof(p->active_path),
                    p->opts.dir_path, p->opts.period, pkey, p->part_seq++);
    p->active_period_key = pkey;

    const char *names[STRIQ_MAX_COLS];
    striq_coltype_t types[STRIQ_MAX_COLS];
    for (uint32_t c = 0; c < p->opts.num_cols; c++) {
        names[c] = p->opts.cols[c].name;
        types[c] = p->opts.cols[c].type;
    }

    uint32_t warm_mb = p->opts.warm_max_memory_mb > 0.0
                     ? (uint32_t)p->opts.warm_max_memory_mb : 16u;
    uint32_t warm_bl = p->opts.warm_max_blocks > 0
                     ? p->opts.warm_max_blocks : 256u;

    striq_store_opts_t sopts = {
        .epsilon_b          = p->opts.epsilon_b,
        .warm_max_blocks    = warm_bl,
        .warm_max_memory_mb = warm_mb,
        .cold_path          = p->active_path,
    };

    p->active = striq_store_create(names, types, p->opts.num_cols, &sopts);
    if (!p->active) return STRIQ_ERR_MEMORY;
    return STRIQ_OK;
}

striq_status_t striq_partition_open(
    striq_partition_t       **out,
    const striq_partition_opts_t *opts)
{
    if (!out || !opts || opts->num_cols == 0) return STRIQ_ERR_PARAM;

    striq_status_t rc = ensure_dir(opts->dir_path);
    if (rc != STRIQ_OK) return rc;

    striq_partition_t *p = (striq_partition_t *)calloc(1, sizeof(*p));
    if (!p) return STRIQ_ERR_MEMORY;

    p->opts              = *opts;
    p->active_period_key = INT64_MIN;

    rc = manifest_load(&p->manifest, opts->dir_path);
    if (rc != STRIQ_OK) {
        rc = manifest_init(&p->manifest, opts->dir_path, opts->period,
                           opts->cols, opts->num_cols, opts->epsilon_b);
        if (rc != STRIQ_OK) { free(p); return rc; }
    }

    *out = p;
    return STRIQ_OK;
}

striq_status_t striq_partition_push(
    striq_partition_t *p,
    int64_t            ts,
    const double      *values,
    uint32_t           num_values)
{
    if (!p || !values) return STRIQ_ERR_PARAM;
    if (num_values != p->opts.num_cols) return STRIQ_ERR_PARAM;

    int64_t pkey = period_key(ts, p->opts.period);

    bool rollover = (p->active_period_key != INT64_MIN && pkey != p->active_period_key);
    if (!rollover && p->opts.cold_rows_per_part > 0 && p->active)
        rollover = (p->active_rows >= p->opts.cold_rows_per_part);

    if (rollover) {
        striq_status_t rc = finalize_active(p);
        if (rc != STRIQ_OK) return rc;
    }

    if (!p->active) {
        striq_status_t rc = open_active(p, pkey);
        if (rc != STRIQ_OK) return rc;
    }

    striq_status_t rc = striq_store_push(p->active, ts, values, num_values);
    if (rc == STRIQ_OK) p->active_rows++;
    return rc;
}

striq_status_t striq_partition_sync(striq_partition_t *p)
{
    if (!p) return STRIQ_ERR_PARAM;
    if (p->active) {
        striq_status_t rc = striq_store_sync(p->active);
        if (rc != STRIQ_OK) return rc;
    }
    return manifest_save(&p->manifest);
}

striq_status_t striq_partition_close(striq_partition_t *p)
{
    if (!p) return STRIQ_ERR_PARAM;
    striq_status_t rc = finalize_active(p);
    free(p);
    return rc;
}


striq_status_t striq_partition_compact(
    striq_partition_t *p,
    int64_t            ts_from,
    int64_t            ts_to)
{
    if (!p) return STRIQ_ERR_PARAM;
    if (p->manifest.num_parts <= 1) return STRIQ_OK;

    uint32_t *cand = (uint32_t *)malloc(p->manifest.num_parts * sizeof(uint32_t));
    if (!cand) return STRIQ_ERR_MEMORY;
    uint32_t ncand = 0;
    for (uint32_t i = 0; i < p->manifest.num_parts; i++) {
        if (entry_overlaps(&p->manifest.parts[i], ts_from, ts_to))
            cand[ncand++] = i;
    }
    if (ncand <= 1) { free(cand); return STRIQ_OK; }

    uint32_t nc = p->opts.num_cols;
    striq_status_t rc = STRIQ_OK;
    char *out_path          = NULL;
    char *full              = NULL;
    striq_col_schema_t *schema = NULL;
    char (*old_paths)[780]  = NULL;
    int64_t *ts_buf         = NULL;
    double *row_vals        = NULL;
    double *col_bufs[STRIQ_MAX_COLS];
    for (uint32_t c = 0; c < STRIQ_MAX_COLS; c++) col_bufs[c] = NULL;

    ts_buf   = (int64_t *)malloc(ENCODER_MAX_ROWS_PER_BLOCK * sizeof(int64_t));
    row_vals = (double *)malloc(nc * sizeof(double));
    out_path = (char *)malloc(780);
    full     = (char *)malloc(780);
    schema   = (striq_col_schema_t *)calloc(STRIQ_MAX_COLS, sizeof(striq_col_schema_t));

    for (uint32_t c = 0; c < nc; c++)
        col_bufs[c] = (double *)malloc(ENCODER_MAX_ROWS_PER_BLOCK * sizeof(double));

    if (!ts_buf || !row_vals || !out_path || !full || !schema) {
        rc = STRIQ_ERR_MEMORY; goto cleanup;
    }
    for (uint32_t c = 0; c < nc; c++) {
        if (!col_bufs[c]) { rc = STRIQ_ERR_MEMORY; goto cleanup; }
    }

    build_part_path(out_path, 780,
                    p->opts.dir_path, p->opts.period,
                    p->manifest.parts[cand[0]].ts_first / 1000000000LL,
                    p->part_seq++);

    FILE *out_fp = fopen(out_path, "wb");
    if (!out_fp) { rc = STRIQ_ERR_IO; goto cleanup; }

    striq_file_ctx_t out_fctx = { .fp = out_fp };

    for (uint32_t c = 0; c < nc; c++) {
        strncpy(schema[c].name, p->opts.cols[c].name, sizeof(schema[c].name) - 1);
        schema[c].type = p->opts.cols[c].type;
    }

    striq_opts_t enc_opts = striq_opts_make();
    enc_opts.epsilon = p->opts.epsilon_b;

    striq_encoder_t *enc = malloc(sizeof(*enc));
    if (!enc) { fclose(out_fp); rc = STRIQ_ERR_MEMORY; goto cleanup; }
    rc = encoder_init(enc, schema, nc, &enc_opts,
                      file_io_write, &out_fctx);
    if (rc != STRIQ_OK) { free(enc); fclose(out_fp); goto cleanup; }

    for (uint32_t ci = 0; ci < ncand; ci++) {
        const striq_manifest_entry_t *ent = &p->manifest.parts[cand[ci]];
        if (!full) { continue; }
        snprintf(full, 780, "%s/%s", p->opts.dir_path, ent->path);

        FILE *in_fp = fopen(full, "rb");
        if (!in_fp) continue;

        striq_file_ctx_t in_fctx = { .fp = in_fp };
        uint64_t fsz = file_io_size(in_fp);

        striq_decoder_t *dec = malloc(sizeof(*dec));
        if (!dec) { fclose(in_fp); continue; }
        if (decoder_init(dec, file_io_read, file_io_seek, &in_fctx, fsz) != STRIQ_OK) {
            free(dec); fclose(in_fp);
            continue;
        }

        for (uint32_t bi = 0; bi < dec->fmt.num_blocks; bi++) {
            uint32_t nrows = dec->fmt.block_index[bi].num_rows;
            if (nrows == 0) continue;

            if (decoder_read_timestamps(dec, bi, ts_buf, ENCODER_MAX_ROWS_PER_BLOCK) != STRIQ_OK)
                continue;

            bool col_ok = true;
            for (uint32_t c = 0; c < nc; c++) {
                if (decoder_read_column(dec, bi, c, col_bufs[c], ENCODER_MAX_ROWS_PER_BLOCK) != STRIQ_OK) {
                    col_ok = false;
                    break;
                }
            }
            if (!col_ok) continue;

            for (uint32_t r = 0; r < nrows; r++) {
                for (uint32_t c = 0; c < nc; c++)
                    row_vals[c] = col_bufs[c][r];
                if (encoder_add_row(enc, ts_buf[r], row_vals, nc) != STRIQ_OK)
                    goto enc_done;
            }
        }
enc_done:
        free(dec);
        fclose(in_fp);
    }

    rc = encoder_close(enc);
    free(enc);
    fclose(out_fp);
    if (rc != STRIQ_OK) goto cleanup;

    struct stat st;
    uint64_t out_sz = 0;
    if (stat(out_path, &st) == 0) out_sz = (uint64_t)st.st_size;

    striq_manifest_entry_t merged;
    memset(&merged, 0, sizeof(merged));
    const char *mbase = strrchr(out_path, '/');
    mbase = mbase ? mbase + 1 : out_path;
    size_t mlen = strlen(mbase);
    if (mlen >= MANIFEST_PATH_MAX) mlen = MANIFEST_PATH_MAX - 1;
    memcpy(merged.path, mbase, mlen);
    merged.ts_first  = p->manifest.parts[cand[0]].ts_first;
    merged.ts_last   = p->manifest.parts[cand[ncand - 1]].ts_last;
    merged.file_size = out_sz;

    for (uint32_t ci = 0; ci < ncand; ci++) {
        const striq_manifest_entry_t *ent = &p->manifest.parts[cand[ci]];
        merged.num_rows += ent->num_rows;
        if (ent->ts_first < merged.ts_first) merged.ts_first = ent->ts_first;
        if (ent->ts_last  > merged.ts_last)  merged.ts_last  = ent->ts_last;
        for (uint32_t c = 0; c < nc && c < STRIQ_MAX_COLS; c++) {
            if (ci == 0) {
                merged.col_stats[c] = ent->col_stats[c];
            } else {
                if (ent->col_stats[c].min < merged.col_stats[c].min)
                    merged.col_stats[c].min = ent->col_stats[c].min;
                if (ent->col_stats[c].max > merged.col_stats[c].max)
                    merged.col_stats[c].max = ent->col_stats[c].max;
                merged.col_stats[c].sum   += ent->col_stats[c].sum;
                merged.col_stats[c].count += ent->col_stats[c].count;
            }
        }
    }

    old_paths = (char (*)[780])malloc(ncand * 780);
    if (!old_paths) { rc = STRIQ_ERR_MEMORY; goto cleanup; }
    for (uint32_t ci = 0; ci < ncand; ci++) {
        snprintf(old_paths[ci], 780, "%s/%s",
                 p->opts.dir_path, p->manifest.parts[cand[ci]].path);
    }

    /* Remove old entries from manifest in-place (walk backwards to preserve indices) */
    for (int ci = (int)ncand - 1; ci >= 0; ci--) {
        uint32_t idx = cand[ci];
        uint32_t tail = p->manifest.num_parts - idx - 1;
        if (tail > 0)
            memmove(&p->manifest.parts[idx],
                    &p->manifest.parts[idx + 1],
                    tail * sizeof(striq_manifest_entry_t));
        p->manifest.num_parts--;
        /* Fix up remaining candidate indices that shifted down */
        for (uint32_t cj = (uint32_t)ci + 1; cj < ncand; cj++) {
            if (cand[cj] > idx) cand[cj]--;
        }
    }

    manifest_upsert(&p->manifest, &merged);

    for (uint32_t ci = 0; ci < ncand; ci++)
        remove(old_paths[ci]);

    manifest_save(&p->manifest);

cleanup:
    free(old_paths);
    free(full);
    free(schema);
    free(out_path);
    free(cand);
    free(ts_buf);
    free(row_vals);
    for (uint32_t c = 0; c < nc; c++) free(col_bufs[c]);
    return rc;
}

typedef struct {
    striq_fmt_reader_t    rdr;
    striq_file_provider_t prov;
    striq_query_engine_t  qe;
    striq_file_ctx_t      fctx;
    FILE                 *fp;
} part_qctx_t;

static bool entry_overlaps(
    const striq_manifest_entry_t *e, int64_t ts_from, int64_t ts_to)
{
    if (ts_to   != 0 && e->ts_first > ts_to)  return false;
    if (ts_from != 0 && e->ts_last  < ts_from) return false;
    return true;
}

static striq_status_t open_part_query(
    part_qctx_t *q,
    const striq_partition_t *p,
    const striq_manifest_entry_t *e)
{
    char full[780];
    snprintf(full, sizeof(full), "%s/%s", p->opts.dir_path, e->path);
    q->fp = fopen(full, "rb");
    if (!q->fp) return STRIQ_ERR_IO;
    q->fctx.fp = q->fp;
    uint64_t fsz = file_io_size(q->fp);
    striq_status_t rc = fmt_reader_open(
        &q->rdr, file_io_read, file_io_seek, &q->fctx, fsz);
    if (rc != STRIQ_OK) { fclose(q->fp); return rc; }
    file_provider_init(&q->prov, &q->rdr);
    engine_init(&q->qe, &q->prov.base);
    return STRIQ_OK;
}

striq_status_t striq_partition_query_mean(
    striq_partition_t      *p,
    const char             *col_name,
    int64_t                 ts_from,
    int64_t                 ts_to,
    striq_query_result_t   *out)
{
    if (!p || !col_name || !out) return STRIQ_ERR_PARAM;
    memset(out, 0, sizeof(*out));

    double   wsum  = 0.0;
    uint64_t total = 0;

    for (uint32_t i = 0; i < p->manifest.num_parts; i++) {
        const striq_manifest_entry_t *e = &p->manifest.parts[i];
        if (!entry_overlaps(e, ts_from, ts_to)) continue;

        part_qctx_t *q = malloc(sizeof(*q));
        if (!q) continue;
        if (open_part_query(q, p, e) != STRIQ_OK) { free(q); continue; }

        striq_query_result_t r;
        if (engine_query_mean(&q->qe, col_name, ts_from, ts_to, &r) == STRIQ_OK
            && r.rows_scanned > 0) {
            wsum  += r.value * (double)r.rows_scanned;
            total += r.rows_scanned;
        }
        fclose(q->fp);
        free(q);
    }

    if (p->active) {
        striq_result_t sr;
        if (striq_store_query_mean(p->active, col_name, ts_from, ts_to, &sr) == STRIQ_OK) {
            wsum  += sr.value * (double)sr.rows_scanned;
            total += sr.rows_scanned;
        }
    }

    if (total == 0) return STRIQ_ERR_NOTFOUND;
    out->value        = wsum / (double)total;
    out->rows_scanned = total;
    return STRIQ_OK;
}

striq_status_t striq_partition_query_count(
    striq_partition_t      *p,
    int64_t                 ts_from,
    int64_t                 ts_to,
    striq_query_result_t   *out)
{
    if (!p || !out) return STRIQ_ERR_PARAM;
    memset(out, 0, sizeof(*out));

    uint64_t total = 0;

    for (uint32_t i = 0; i < p->manifest.num_parts; i++) {
        const striq_manifest_entry_t *e = &p->manifest.parts[i];
        if (!entry_overlaps(e, ts_from, ts_to)) continue;

        part_qctx_t *q = malloc(sizeof(*q));
        if (!q) continue;
        if (open_part_query(q, p, e) != STRIQ_OK) { free(q); continue; }

        uint64_t cnt = 0;
        if (engine_query_count(&q->qe, ts_from, ts_to, &cnt) == STRIQ_OK)
            total += cnt;
        fclose(q->fp);
        free(q);
    }

    if (p->active) {
        uint64_t cnt = 0;
        if (striq_store_query_count(p->active, ts_from, ts_to, &cnt) == STRIQ_OK)
            total += cnt;
    }

    out->value        = (double)total;
    out->rows_scanned = total;
    return STRIQ_OK;
}

striq_status_t striq_partition_query_min(
    striq_partition_t      *p,
    const char             *col_name,
    int64_t                 ts_from,
    int64_t                 ts_to,
    striq_query_result_t   *out)
{
    if (!p || !col_name || !out) return STRIQ_ERR_PARAM;
    memset(out, 0, sizeof(*out));

    double global_min = 1e300;
    bool   found      = false;

    for (uint32_t i = 0; i < p->manifest.num_parts; i++) {
        const striq_manifest_entry_t *e = &p->manifest.parts[i];
        if (!entry_overlaps(e, ts_from, ts_to)) continue;

        part_qctx_t *q = malloc(sizeof(*q));
        if (!q) continue;
        if (open_part_query(q, p, e) != STRIQ_OK) { free(q); continue; }

        striq_query_result_t r;
        if (engine_query_min(&q->qe, col_name, ts_from, ts_to, &r) == STRIQ_OK) {
            if (!found || r.value < global_min) global_min = r.value;
            found = true;
            out->rows_scanned += r.rows_scanned;
        }
        fclose(q->fp);
        free(q);
    }

    if (p->active) {
        striq_result_t sr;
        if (striq_store_query_min(p->active, col_name, ts_from, ts_to, &sr) == STRIQ_OK) {
            if (!found || sr.value < global_min) global_min = sr.value;
            found = true;
            out->rows_scanned += sr.rows_scanned;
        }
    }

    if (!found) return STRIQ_ERR_NOTFOUND;
    out->value = global_min;
    return STRIQ_OK;
}

striq_status_t striq_partition_query_max(
    striq_partition_t      *p,
    const char             *col_name,
    int64_t                 ts_from,
    int64_t                 ts_to,
    striq_query_result_t   *out)
{
    if (!p || !col_name || !out) return STRIQ_ERR_PARAM;
    memset(out, 0, sizeof(*out));

    double global_max = -1e300;
    bool   found      = false;

    for (uint32_t i = 0; i < p->manifest.num_parts; i++) {
        const striq_manifest_entry_t *e = &p->manifest.parts[i];
        if (!entry_overlaps(e, ts_from, ts_to)) continue;

        part_qctx_t *q = malloc(sizeof(*q));
        if (!q) continue;
        if (open_part_query(q, p, e) != STRIQ_OK) { free(q); continue; }

        striq_query_result_t r;
        if (engine_query_max(&q->qe, col_name, ts_from, ts_to, &r) == STRIQ_OK) {
            if (!found || r.value > global_max) global_max = r.value;
            found = true;
            out->rows_scanned += r.rows_scanned;
        }
        fclose(q->fp);
        free(q);
    }

    if (p->active) {
        striq_result_t sr;
        if (striq_store_query_max(p->active, col_name, ts_from, ts_to, &sr) == STRIQ_OK) {
            if (!found || sr.value > global_max) global_max = sr.value;
            found = true;
            out->rows_scanned += sr.rows_scanned;
        }
    }

    if (!found) return STRIQ_ERR_NOTFOUND;
    out->value = global_max;
    return STRIQ_OK;
}

uint32_t striq_partition_num_parts(const striq_partition_t *p)
{
    return p ? p->manifest.num_parts : 0u;
}
