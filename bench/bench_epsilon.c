#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>

#include "../include/striq.h"
#include "harness/csv_loader.h"
#include "harness/bench.h"

#define DATASET_PATH     "bench/datasets/jena_climate_2009_2016.csv"
#define OUT_PATH         "bench/results/epsilon.csv"
#define TMP_PATH         "/tmp/bench_eps_striq.striq"
#define N_QUERY_REPS     20

static const double EPS_PCT[] = { 0.0, 0.01, 0.1, 0.5, 1.0 };  /* % of range */
static const int    N_EPS     = 5;

static const char *TARGET_COLS[] = {
    "\"T (degC)\"", "\"p (mbar)\"", "\"rh (%)\"", "\"Tpot (K)\"",
    "\"Tdew (degC)\"", "\"VPmax (mbar)\"", "\"H2OC (mmol/mol)\"",
    "\"max. wv (m/s)\"", "\"wv (m/s)\""
};
static const int N_TARGET_COLS = 9;

static double now_s(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

typedef struct {
    double ratio;
    double query_mean_us;
    double max_err;
    char   codec[32];
} eps_result_t;

static int bench_one(const char *col_name, const double *values, uint32_t N,
                     double epsilon, eps_result_t *out)
{
    const char     *col_names[] = { col_name };
    striq_coltype_t col_types[] = { STRIQ_COL_FLOAT64 };

    remove(TMP_PATH);
    striq_opts_t wopts = striq_opts_make(); wopts.epsilon = epsilon;
    striq_writer_t *w = striq_writer_open(TMP_PATH, col_names, col_types, 1, &wopts);
    if (!w) return -1;

    for (uint32_t i = 0; i < N; i++) {
        int64_t ts = (int64_t)i + 1;
        striq_writer_add_row(w, ts, &values[i], 1);
    }
    striq_writer_close(w);

    FILE *f = fopen(TMP_PATH, "rb");
    if (!f) return -1;
    fseek(f, 0, SEEK_END);
    size_t csz = (size_t)ftell(f);
    fclose(f);

    size_t raw = (size_t)N * 8;
    out->ratio = (csz > 0) ? (double)raw / (double)csz : 0.0;

    double max_err = 0.0;
    {
        striq_reader_t *rd = striq_reader_open(TMP_PATH);
        if (rd) {
            double  *ds_out = malloc((size_t)N * sizeof(double));
            int64_t *ds_ts  = malloc((size_t)N * sizeof(int64_t));
            if (ds_out && ds_ts) {
                striq_query_downsample(rd, col_name, 0, 0, N, ds_out, ds_ts);
                for (uint32_t i = 0; i < N; i++) {
                    int64_t ts = ds_ts[i];
                    if (ts > 0 && (uint32_t)(ts - 1) < N) {
                        uint32_t orig = (uint32_t)(ts - 1);
                        double e = fabs(ds_out[i] - values[orig]);
                        if (e > max_err) max_err = e;
                    }
                }
            }
            free(ds_out);
            free(ds_ts);
            striq_reader_close(rd);
        }
    }
    out->max_err = max_err;

    striq_reader_t *rd = striq_reader_open(TMP_PATH);
    if (!rd) return -1;

    striq_result_t qr = {0};
    double t0 = now_s();
    for (int r = 0; r < N_QUERY_REPS; r++)
        striq_query_mean(rd, col_name, 0, 0, &qr);
    double query_us = (now_s() - t0) / N_QUERY_REPS * 1e6;
    striq_reader_close(rd);

    out->query_mean_us = query_us;

    if (N <= 64) {
        snprintf(out->codec, sizeof out->codec, "RLE");
    } else if (epsilon == 0.0) {
        snprintf(out->codec, sizeof out->codec, "RAW_STATS");
    } else if (out->ratio > 5.0) {
        snprintf(out->codec, sizeof out->codec, "PLA");
    } else if (out->ratio > 2.0) {
        snprintf(out->codec, sizeof out->codec, "PLA_CHEB");
    } else {
        snprintf(out->codec, sizeof out->codec, "RAW_STATS");
    }

    remove(TMP_PATH);
    return 0;
}

static void col_range(const double *v, uint32_t n, double *mn, double *mx)
{
    *mn = v[0]; *mx = v[0];
    for (uint32_t i = 1; i < n; i++) {
        if (v[i] < *mn) *mn = v[i];
        if (v[i] > *mx) *mx = v[i];
    }
}

int main(void)
{
    printf("=== STRIQ Phase 6 Epsilon Benchmark ===\n");
    printf("Dataset: %s\n", DATASET_PATH);

    csv_dataset_t *ds = csv_load(DATASET_PATH);
    if (!ds) {
        fprintf(stderr, "ERROR: could not load %s\n", DATASET_PATH);
        return 1;
    }
    printf("Loaded: %u rows, %u columns\n", ds->nrows, ds->ncols);

    FILE *out = fopen(OUT_PATH, "w");
    if (!out) {
        fprintf(stderr, "ERROR: could not open %s for writing\n", OUT_PATH);
        csv_free(ds);
        return 1;
    }
    fprintf(out, "dataset,column,N,epsilon_pct,epsilon_abs,ratio,"
                 "max_err,query_mean_us,codec\n");

    int total = 0;

    for (uint32_t ci = 0; ci < ds->ncols; ci++) {
        int is_target = 0;
        for (int ti = 0; ti < N_TARGET_COLS; ti++) {
            if (strcmp(ds->col_names[ci], TARGET_COLS[ti]) == 0) {
                is_target = 1; break;
            }
        }
        if (!is_target) continue;

        const char *col_name = ds->col_names[ci];
        const double *values = ds->columns[ci];
        uint32_t N = ds->nrows;

        double col_min, col_max;
        col_range(values, N, &col_min, &col_max);
        double range = col_max - col_min;

        printf("\nColumn: %s  N=%u  range=[%.3f, %.3f]\n",
               col_name, N, col_min, col_max);

        for (int ei = 0; ei < N_EPS; ei++) {
            double eps_pct = EPS_PCT[ei];
            double epsilon  = eps_pct / 100.0 * range;

            printf("  ε=%.2f%% (%.4f) ... ", eps_pct, epsilon);
            fflush(stdout);

            eps_result_t r = {0};
            if (bench_one(col_name, values, N, epsilon, &r) != 0) {
                printf("FAILED\n");
                continue;
            }

            printf("ratio=%.2fx  query_mean=%.1fμs  max_err=%.4f  codec=%s\n",
                   r.ratio, r.query_mean_us, r.max_err, r.codec);

            fprintf(out, "jena,%s,%u,%.4f,%.6f,%.4f,%.6f,%.2f,%s\n",
                    col_name, N, eps_pct, epsilon,
                    r.ratio, r.max_err, r.query_mean_us, r.codec);
            total++;
        }
    }

    fclose(out);
    csv_free(ds);

    printf("\n=== Written %d rows to %s ===\n", total, OUT_PATH);
    return 0;
}
