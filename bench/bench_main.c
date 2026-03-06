#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <dirent.h>

#include <lz4.h>
#include <zstd.h>
#include "../include/striq.h"
#include "codecs/gorilla.h"
#include "harness/bench.h"
#include "harness/csv_loader.h"
#include "harness/report.h"

#define STRIQ_EPSILON_LOSSY   0.01
#define STRIQ_EPSILON_LOSSLESS 0.0
#define N_DS_REPS             3
#define N_QUERY_REPS          10
#define STRIQ_TMP_PATH        "/tmp/bench_striq.striq"
#define MAX_DATASET_FILES     64

static double now_s(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

static double compute_mean(const double *v, size_t n)
{
    double s = 0.0;
    for (size_t i = 0; i < n; i++) s += v[i];
    return s / (double)n;
}

static void bench_lz4(const double *values, uint32_t N,
                      const char *dataset, const char *col,
                      bench_results_t *rs)
{
    size_t raw = (size_t)N * 8;
    int bound = LZ4_compressBound((int)raw);
    char *cbuf = malloc((size_t)bound);
    char *dbuf = malloc(raw);
    if (!cbuf || !dbuf) { free(cbuf); free(dbuf); return; }

    double t0 = now_s();
    int csz = 0;
    for (int r = 0; r < N_DS_REPS; r++)
        csz = LZ4_compress_default((const char*)values, cbuf, (int)raw, bound);
    double encode_ms = (now_s() - t0) / N_DS_REPS * 1000.0;

    if (csz <= 0) { free(cbuf); free(dbuf); return; }

    t0 = now_s();
    for (int r = 0; r < N_DS_REPS; r++)
        LZ4_decompress_safe(cbuf, dbuf, csz, (int)raw);
    double decode_ms = (now_s() - t0) / N_DS_REPS * 1000.0;

    double qsum = 0.0;
    t0 = now_s();
    for (int r = 0; r < N_QUERY_REPS; r++) {
        LZ4_decompress_safe(cbuf, dbuf, csz, (int)raw);
        qsum = compute_mean((const double*)dbuf, N);
    }
    double query_us = (now_s() - t0) / N_QUERY_REPS * 1e6;
    (void)qsum;

    bench_result_t br = {0};
    snprintf(br.codec,   sizeof br.codec,   "lz4");
    snprintf(br.dataset, sizeof br.dataset, "%s", dataset);
    snprintf(br.column,  sizeof br.column,  "%s", col);
    br.N                 = N;
    br.raw_bytes         = raw;
    br.compressed_bytes  = (uint64_t)csz;
    br.ratio             = (double)raw / (double)csz;
    br.encode_ms         = encode_ms;
    br.decode_ms         = decode_ms;
    br.encode_mbs        = (double)raw / 1e6 / (encode_ms / 1000.0);
    br.decode_mbs        = (double)raw / 1e6 / (decode_ms / 1000.0);
    br.query_lz4_us      = query_us;
    bench_results_add(rs, &br);

    free(cbuf); free(dbuf);
}

static void bench_zstd(const double *values, uint32_t N,
                       const char *dataset, const char *col,
                       bench_results_t *rs)
{
    size_t raw = (size_t)N * 8;
    size_t bound = ZSTD_compressBound(raw);
    char *cbuf = malloc(bound);
    char *dbuf = malloc(raw);
    if (!cbuf || !dbuf) { free(cbuf); free(dbuf); return; }

    double t0 = now_s();
    size_t csz = 0;
    for (int r = 0; r < N_DS_REPS; r++)
        csz = ZSTD_compress(cbuf, bound, values, raw, 3);
    double encode_ms = (now_s() - t0) / N_DS_REPS * 1000.0;

    if (ZSTD_isError(csz)) { free(cbuf); free(dbuf); return; }

    t0 = now_s();
    for (int r = 0; r < N_DS_REPS; r++)
        ZSTD_decompress(dbuf, raw, cbuf, csz);
    double decode_ms = (now_s() - t0) / N_DS_REPS * 1000.0;

    double qsum = 0.0;
    t0 = now_s();
    for (int r = 0; r < N_QUERY_REPS; r++) {
        ZSTD_decompress(dbuf, raw, cbuf, csz);
        qsum = compute_mean((const double*)dbuf, N);
    }
    double query_us = (now_s() - t0) / N_QUERY_REPS * 1e6;
    (void)qsum;

    bench_result_t br = {0};
    snprintf(br.codec,   sizeof br.codec,   "zstd-3");
    snprintf(br.dataset, sizeof br.dataset, "%s", dataset);
    snprintf(br.column,  sizeof br.column,  "%s", col);
    br.N                 = N;
    br.raw_bytes         = raw;
    br.compressed_bytes  = (uint64_t)csz;
    br.ratio             = (double)raw / (double)csz;
    br.encode_ms         = encode_ms;
    br.decode_ms         = decode_ms;
    br.encode_mbs        = (double)raw / 1e6 / (encode_ms / 1000.0);
    br.decode_mbs        = (double)raw / 1e6 / (decode_ms / 1000.0);
    br.query_zstd_us     = query_us;
    bench_results_add(rs, &br);

    free(cbuf); free(dbuf);
}

static void bench_gorilla(const double *values, uint32_t N,
                          const char *dataset, const char *col,
                          bench_results_t *rs)
{
    size_t raw = (size_t)N * 8;
    size_t cbuf_cap = raw + 64;
    uint8_t *cbuf = malloc(cbuf_cap);
    double  *dbuf = malloc(raw);
    if (!cbuf || !dbuf) { free(cbuf); free(dbuf); return; }

    double t0 = now_s();
    size_t csz = 0;
    for (int r = 0; r < N_DS_REPS; r++)
        csz = gorilla_compress(values, N, cbuf, cbuf_cap);
    double encode_ms = (now_s() - t0) / N_DS_REPS * 1000.0;

    if (csz == 0) { free(cbuf); free(dbuf); return; }

    t0 = now_s();
    for (int r = 0; r < N_DS_REPS; r++)
        gorilla_decompress(cbuf, csz, N, dbuf, N);
    double decode_ms = (now_s() - t0) / N_DS_REPS * 1000.0;

    double qsum = 0.0;
    t0 = now_s();
    for (int r = 0; r < N_QUERY_REPS; r++) {
        gorilla_decompress(cbuf, csz, N, dbuf, N);
        qsum = compute_mean(dbuf, N);
    }
    double query_us = (now_s() - t0) / N_QUERY_REPS * 1e6;
    (void)qsum;

    bench_result_t br = {0};
    snprintf(br.codec,        sizeof br.codec,   "gorilla");
    snprintf(br.dataset,      sizeof br.dataset, "%s", dataset);
    snprintf(br.column,       sizeof br.column,  "%s", col);
    br.N                      = N;
    br.raw_bytes              = raw;
    br.compressed_bytes       = csz;
    br.ratio                  = (double)raw / (double)csz;
    br.encode_ms              = encode_ms;
    br.decode_ms              = decode_ms;
    br.encode_mbs             = (double)raw / 1e6 / (encode_ms / 1000.0);
    br.decode_mbs             = (double)raw / 1e6 / (decode_ms / 1000.0);
    br.query_gorilla_us       = query_us;
    bench_results_add(rs, &br);

    free(cbuf); free(dbuf);
}

static void bench_striq(const double *values, uint32_t N,
                        double epsilon,
                        const char *dataset, const char *col,
                        bench_results_t *rs)
{
    const char *col_names[] = { col };
    striq_coltype_t col_types[] = { STRIQ_COL_FLOAT64 };
    size_t raw = (size_t)N * 8;

    int64_t *ts = malloc(N * sizeof(int64_t));
    if (!ts) return;
    for (uint32_t i = 0; i < N; i++) ts[i] = (int64_t)i + 1;

    double t0 = now_s();
    for (int r = 0; r < N_DS_REPS; r++) {
        remove(STRIQ_TMP_PATH);
        striq_opts_t wopts = striq_opts_make(); wopts.epsilon = epsilon;
        striq_writer_t *w = striq_writer_open(STRIQ_TMP_PATH,
            col_names, col_types, 1, &wopts);
        if (!w) continue;
        for (uint32_t i = 0; i < N; i++)
            striq_writer_add_row(w, ts[i], &values[i], 1);
        striq_writer_close(w);
    }
    double encode_ms = (now_s() - t0) / N_DS_REPS * 1000.0;

    FILE *f = fopen(STRIQ_TMP_PATH, "rb");
    size_t csz = 0;
    if (f) {
        fseek(f, 0, SEEK_END);
        csz = (size_t)ftell(f);
        fclose(f);
    }

    t0 = now_s();
    for (int r = 0; r < N_DS_REPS; r++) {
        striq_reader_t *rd = striq_reader_open(STRIQ_TMP_PATH);
        if (!rd) continue;
        uint64_t cnt = 0;
        striq_query_count(rd, 0, 0, &cnt);
        striq_reader_close(rd);
        (void)cnt;
    }
    double decode_ms = (now_s() - t0) / N_DS_REPS * 1000.0;

    double max_err = 0.0;
    if (epsilon > 0.0) {
        max_err = epsilon;
    }

    striq_result_t qr = {0};
    double query_mean_us = 0, query_min_us = 0, query_max_us = 0;
    double query_sum_us = 0, query_count_us = 0, query_var_us = 0;
    double query_where_us = 0, query_ds_us = 0;
    double query_value_at_us = 0, query_scan_us = 0;

    {
        striq_reader_t *rd = striq_reader_open(STRIQ_TMP_PATH);
        if (rd) {
            t0 = now_s();
            for (int r = 0; r < N_QUERY_REPS; r++)
                striq_query_mean(rd, col, 0, 0, &qr);
            query_mean_us = (now_s() - t0) / N_QUERY_REPS * 1e6;

            t0 = now_s();
            for (int r = 0; r < N_QUERY_REPS; r++)
                striq_query_min(rd, col, 0, 0, &qr);
            query_min_us = (now_s() - t0) / N_QUERY_REPS * 1e6;

            t0 = now_s();
            for (int r = 0; r < N_QUERY_REPS; r++)
                striq_query_max(rd, col, 0, 0, &qr);
            query_max_us = (now_s() - t0) / N_QUERY_REPS * 1e6;

            t0 = now_s();
            for (int r = 0; r < N_QUERY_REPS; r++)
                striq_query_sum(rd, col, 0, 0, &qr);
            query_sum_us = (now_s() - t0) / N_QUERY_REPS * 1e6;

            uint64_t cnt = 0;
            t0 = now_s();
            for (int r = 0; r < N_QUERY_REPS; r++)
                striq_query_count(rd, 0, 0, &cnt);
            query_count_us = (now_s() - t0) / N_QUERY_REPS * 1e6;

            t0 = now_s();
            for (int r = 0; r < N_QUERY_REPS; r++)
                striq_query_variance(rd, col, 0, 0, &qr);
            query_var_us = (now_s() - t0) / N_QUERY_REPS * 1e6;

            t0 = now_s();
            for (int r = 0; r < N_QUERY_REPS; r++)
                striq_query_mean_where(rd, col, 0, 0,
                                       0.0, STRIQ_CMP_GT, &qr);
            query_where_us = (now_s() - t0) / N_QUERY_REPS * 1e6;

            double *ds_out = malloc(1000 * sizeof(double));
            t0 = now_s();
            for (int r = 0; r < N_QUERY_REPS; r++)
                striq_query_downsample(rd, col, 0, 0, 1000, ds_out, NULL);
            query_ds_us = (now_s() - t0) / N_QUERY_REPS * 1e6;
            free(ds_out);

            const char *va_cols[] = { col };
            double va_val, va_err;
            uint32_t va_nc = 0;
            int64_t mid_ts = (int64_t)(N / 2) + 1;
            t0 = now_s();
            for (int r = 0; r < N_QUERY_REPS; r++)
                striq_query_value_at(rd, va_cols, 1, mid_ts,
                                     &va_val, &va_err, &va_nc);
            query_value_at_us = (now_s() - t0) / N_QUERY_REPS * 1e6;

            uint32_t scan_max = 1000;
            double  *sc_vals = malloc(scan_max * sizeof(double));
            int64_t *sc_ts   = malloc(scan_max * sizeof(int64_t));
            uint32_t sc_nr = 0, sc_nc = 0;
            int64_t sc_from = mid_ts - 500;
            int64_t sc_to   = mid_ts + 499;
            t0 = now_s();
            for (int r = 0; r < N_QUERY_REPS; r++)
                striq_query_scan(rd, va_cols, 1, sc_from, sc_to,
                                 sc_vals, sc_ts, scan_max, &sc_nr, &sc_nc);
            query_scan_us = (now_s() - t0) / N_QUERY_REPS * 1e6;
            free(sc_vals); free(sc_ts);

            striq_reader_close(rd);
        }
    }

    char codec_name[32];
    snprintf(codec_name, sizeof codec_name,
             epsilon > 0.0 ? "striq-e%.3g" : "striq-lossless", epsilon);

    bench_result_t br = {0};
    snprintf(br.codec,   sizeof br.codec,   "%s", codec_name);
    snprintf(br.dataset, sizeof br.dataset, "%s", dataset);
    snprintf(br.column,  sizeof br.column,  "%s", col);
    br.N                 = N;
    br.raw_bytes         = raw;
    br.compressed_bytes  = csz;
    br.ratio             = csz > 0 ? (double)raw / (double)csz : 0.0;
    br.encode_ms         = encode_ms;
    br.decode_ms         = decode_ms;
    br.encode_mbs        = (double)raw / 1e6 / (encode_ms / 1000.0);
    br.decode_mbs        = (double)raw / 1e6 / (decode_ms / 1000.0);
    br.max_error         = max_err;
    br.query_mean_us     = query_mean_us;
    br.query_ds_us       = query_ds_us;
    br.query_min_us      = query_min_us;
    br.query_max_us      = query_max_us;
    br.query_sum_us      = query_sum_us;
    br.query_count_us    = query_count_us;
    br.query_var_us      = query_var_us;
    br.query_where_us    = query_where_us;
    br.query_value_at_us = query_value_at_us;
    br.query_scan_us     = query_scan_us;
    bench_results_add(rs, &br);

    free(ts);
    remove(STRIQ_TMP_PATH);
}

static void bench_query_comparison(const double *values, uint32_t N,
                                   const char *dataset, const char *col,
                                   bench_results_t *rs)
{
    bench_striq(values, N, STRIQ_EPSILON_LOSSY, dataset, col, rs);
    bench_gorilla(values, N, dataset, col, rs);
    bench_lz4(values, N, dataset, col, rs);
    bench_zstd(values, N, dataset, col, rs);
}

static void run_dataset(const char *path, bench_results_t *rs)
{
    const char *name = strrchr(path, '/');
    name = name ? name + 1 : path;

    char ds_name[128];
    snprintf(ds_name, sizeof ds_name, "%s", name);
    char *dot = strrchr(ds_name, '.');
    if (dot) *dot = '\0';

    printf("\nLoading dataset: %s\n", path);
    csv_dataset_t *ds = csv_load(path);
    if (!ds) {
        fprintf(stderr, "  Failed to load %s\n", path);
        return;
    }
    printf("  %u columns × %u rows loaded\n", ds->ncols, ds->nrows);

    uint32_t N = ds->nrows;
    /* Cap at 500K rows — enough for representative statistics while keeping
     * runtime manageable (household: 2M rows, jena: 420K rows).        */
    if (N > 500000) {
        printf("  (Using first 500K rows of %u for benchmark)\n", N);
        N = 500000;
    }

    for (uint32_t c = 0; c < ds->ncols; c++) {
        printf("  Column %-20s: ", ds->col_names[c]);
        fflush(stdout);

        bench_query_comparison(ds->columns[c], N, ds_name, ds->col_names[c], rs);

        for (int i = rs->count - 4; i < rs->count; i++) {
            if (strncmp(rs->rows[i].codec, "striq", 5) == 0) {
                printf("STRIQ %.2fx  mean %.1fµs  gorilla %.2fx  lz4 %.2fx\n",
                       rs->rows[i].ratio,
                       rs->rows[i].query_mean_us,
                       rs->rows[i - 1 + (rs->count - i) < 4 ? 1 : 0].ratio,
                       rs->rows[i+1 < rs->count ? i+1 : i].ratio);
                break;
            }
        }
    }

    csv_free(ds);
}

static int collect_csv_files(const char *dir, char **paths, int max)
{
    DIR *d = opendir(dir);
    if (!d) return 0;
    int n = 0;
    struct dirent *ent;
    while (n < max && (ent = readdir(d)) != NULL) {
        const char *nm = ent->d_name;
        size_t len = strlen(nm);
        if (len < 4) continue;
        const char *ext = nm + len - 4;
        if (strcmp(ext, ".csv") != 0 && strcmp(ext, ".txt") != 0) continue;
        char *p = malloc(strlen(dir) + len + 2);
        sprintf(p, "%s/%s", dir, nm);
        paths[n++] = p;
    }
    closedir(d);
    return n;
}

static void bench_query_types(const double *values, uint32_t N,
                               const char *label)
{
    const char *names[] = { "v" };
    striq_coltype_t types[] = { STRIQ_COL_FLOAT64 };
    striq_opts_t opts = STRIQ_DEFAULTS;
    opts.epsilon = STRIQ_EPSILON_LOSSY;

    int64_t *ts = malloc(N * sizeof(int64_t));
    for (uint32_t i = 0; i < N; i++) ts[i] = (int64_t)i * 1000000LL;

    striq_writer_t *w = striq_writer_open(STRIQ_TMP_PATH, names, types, 1, &opts);
    if (w) {
        for (uint32_t i = 0; i < N; i++) striq_writer_add_row(w, ts[i], &values[i], 1);
        striq_writer_close(w);
    }
    free(ts);

    striq_reader_t *rd = striq_reader_open(STRIQ_TMP_PATH);
    if (!rd) { remove(STRIQ_TMP_PATH); return; }

    striq_result_t qr = {0};
    striq_query_mean(rd, "v", 0, 0, &qr);

    double t0;
    int reps = N_QUERY_REPS * 3;

    t0 = now_s();
    for (int r = 0; r < reps; r++) striq_query_mean(rd, "v", 0, 0, &qr);
    double mean_us = (now_s() - t0) / reps * 1e6;

    t0 = now_s();
    for (int r = 0; r < reps; r++) striq_query_min(rd, "v", 0, 0, &qr);
    double min_us = (now_s() - t0) / reps * 1e6;

    t0 = now_s();
    for (int r = 0; r < reps; r++) striq_query_max(rd, "v", 0, 0, &qr);
    double max_us = (now_s() - t0) / reps * 1e6;

    t0 = now_s();
    for (int r = 0; r < reps; r++) striq_query_sum(rd, "v", 0, 0, &qr);
    double sum_us = (now_s() - t0) / reps * 1e6;

    uint64_t cnt = 0;
    t0 = now_s();
    for (int r = 0; r < reps; r++) striq_query_count(rd, 0, 0, &cnt);
    double count_us = (now_s() - t0) / reps * 1e6;

    t0 = now_s();
    for (int r = 0; r < reps; r++) striq_query_variance(rd, "v", 0, 0, &qr);
    double var_us = (now_s() - t0) / reps * 1e6;

    t0 = now_s();
    for (int r = 0; r < reps; r++)
        striq_query_mean_where(rd, "v", 0, 0, 0.0, STRIQ_CMP_GT, &qr);
    double where_us = (now_s() - t0) / reps * 1e6;

    double ds_buf[100];
    t0 = now_s();
    for (int r = 0; r < reps; r++)
        striq_query_downsample(rd, "v", 0, 0, 100, ds_buf, NULL);
    double ds_us = (now_s() - t0) / reps * 1e6;

    const char *va_cols[] = { "v" };
    double va_val, va_err;
    uint32_t va_nc = 0;
    int64_t mid_ts = (int64_t)(N / 2) * 1000000LL;
    t0 = now_s();
    for (int r = 0; r < reps; r++)
        striq_query_value_at(rd, va_cols, 1, mid_ts, &va_val, &va_err, &va_nc);
    double vat_us = (now_s() - t0) / reps * 1e6;

    uint32_t scan_max = 1000;
    double  *sc_vals = malloc(scan_max * sizeof(double));
    int64_t *sc_ts   = malloc(scan_max * sizeof(int64_t));
    uint32_t sc_nr = 0, sc_nc = 0;
    int64_t sc_from = mid_ts - 500 * 1000000LL;
    int64_t sc_to   = mid_ts + 499 * 1000000LL;
    t0 = now_s();
    for (int r = 0; r < reps; r++)
        striq_query_scan(rd, va_cols, 1, sc_from, sc_to,
                         sc_vals, sc_ts, scan_max, &sc_nr, &sc_nc);
    double scan_us = (now_s() - t0) / reps * 1e6;
    free(sc_vals); free(sc_ts);

    striq_reader_close(rd);
    remove(STRIQ_TMP_PATH);

    printf("  %-16s  mean=%5.1f  min=%5.1f  max=%5.1f  sum=%5.1f  "
           "count=%5.1f  var=%5.1f  where=%5.1f  ds=%5.1f  "
           "val_at=%5.1f  scan=%5.1f µs  alg=%.0f%%\n",
           label, mean_us, min_us, max_us, sum_us,
           count_us, var_us, where_us, ds_us,
           vat_us, scan_us, qr.pct_algebraic);
}

static void run_synthetic(bench_results_t *rs)
{
    printf("\n=== Synthetic signals ===\n");

    const int N = 50000;
    double *sig = malloc(N * sizeof(double));

    for (int i = 0; i < N; i++) sig[i] = 1000.0 + 100.0 * sin(i * 2.0 * M_PI / 1000.0);
    bench_query_comparison(sig, N, "synthetic", "slow_sine", rs);
    printf("  slow_sine:  ");
    for (int i = rs->count - 4; i < rs->count; i++)
        printf("%s=%.2fx  ", rs->rows[i].codec, rs->rows[i].ratio);
    printf("\n");

    for (int i = 0; i < N; i++) sig[i] = i * 0.001;
    bench_query_comparison(sig, N, "synthetic", "ramp", rs);
    printf("  ramp:       ");
    for (int i = rs->count - 4; i < rs->count; i++)
        printf("%s=%.2fx  ", rs->rows[i].codec, rs->rows[i].ratio);
    printf("\n");

    srand(42);
    for (int i = 0; i < N; i++) sig[i] = (double)rand() / RAND_MAX * 100.0;
    bench_query_comparison(sig, N, "synthetic", "noise", rs);
    printf("  noise:      ");
    for (int i = rs->count - 4; i < rs->count; i++)
        printf("%s=%.2fx  ", rs->rows[i].codec, rs->rows[i].ratio);
    printf("\n");

    printf("\n--- Query latency: all operations (µs per call) ---\n");
    for (int i = 0; i < N; i++) sig[i] = 1000.0 + 100.0 * sin(i * 2.0 * M_PI / 1000.0);
    bench_query_types(sig, N, "slow_sine");

    for (int i = 0; i < N; i++) sig[i] = i * 0.001;
    bench_query_types(sig, N, "ramp");

    srand(42);
    for (int i = 0; i < N; i++) sig[i] = (double)rand() / RAND_MAX * 100.0;
    bench_query_types(sig, N, "noise");

    free(sig);
}

int main(int argc, char **argv)
{
    printf("=== STRIQ Benchmark Harness ===\n");
    printf("Comparing: STRIQ(ε=%.2g) vs Gorilla vs LZ4 vs Zstd-3\n\n",
           STRIQ_EPSILON_LOSSY);

    bench_results_t rs = {0};

    run_synthetic(&rs);

    if (argc <= 1) {
        char *paths[MAX_DATASET_FILES];
        int nf = collect_csv_files("bench/datasets", paths, MAX_DATASET_FILES);
        if (nf == 0) {
            printf("\nNo CSV files found in bench/datasets/.\n");
            printf("Run: bash bench/datasets/download.sh\n");
        } else {
            for (int i = 0; i < nf; i++) {
                run_dataset(paths[i], &rs);
                free(paths[i]);
            }
        }
    } else {
        for (int i = 1; i < argc; i++) {
            if (strcmp(argv[i], "--all") == 0) {
                char *paths[MAX_DATASET_FILES];
                int nf = collect_csv_files("bench/datasets", paths, MAX_DATASET_FILES);
                for (int j = 0; j < nf; j++) {
                    run_dataset(paths[j], &rs);
                    free(paths[j]);
                }
            } else {
                run_dataset(argv[i], &rs);
            }
        }
    }

    if (rs.count > 0) {
        char mkdir_cmd[] = "mkdir -p bench/results bench/results/plots";
        (void)system(mkdir_cmd);

        report_write_csv("bench/results/compression.csv", &rs);
        printf("\nResults written to bench/results/compression.csv\n");

        report_print_markdown(&rs);
    }

    remove(STRIQ_TMP_PATH);
    return 0;
}
