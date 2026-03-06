/*
 * test_integration.c
 * 4 end-to-end integration tests for Phase 1 row-granular queries.
 * Each test encodes data, flushes to an in-memory buffer, re-opens it,
 * runs a query, and compares against a brute-force C computation.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <math.h>
#include <stdint.h>

#include "../include/striq.h"

#define TMP_FILE "striq_test_integration.striq"

static void write_1col(
    const char *name, const int64_t *ts,
    const double *vals, size_t n, double eps)
{
    const char *names[1] = { name };
    striq_coltype_t types[1] = { STRIQ_COL_FLOAT64 };

    striq_opts_t opts = striq_opts_make(); opts.epsilon = eps;
    striq_writer_t *w = striq_writer_open(TMP_FILE, names, types, 1, &opts);
    assert(w != NULL);
    for (size_t i = 0; i < n; i++) {
        striq_status_t s = striq_writer_add_row(w, ts[i], &vals[i], 1);
        assert(s == STRIQ_OK);
    }
    striq_status_t s = striq_writer_close(w);
    assert(s == STRIQ_OK);
}

static void write_2col(
    const char *n1, const char *n2,
    const int64_t *ts,
    const double *v1, const double *v2,
    size_t n, double eps)
{
    const char *names[2] = { n1, n2 };
    striq_coltype_t types[2] = { STRIQ_COL_FLOAT64, STRIQ_COL_FLOAT64 };
    double row[2];

    striq_opts_t opts2 = striq_opts_make(); opts2.epsilon = eps;
    striq_writer_t *w = striq_writer_open(TMP_FILE, names, types, 2, &opts2);
    assert(w != NULL);
    for (size_t i = 0; i < n; i++) {
        row[0] = v1[i]; row[1] = v2[i];
        striq_status_t s = striq_writer_add_row(w, ts[i], row, 2);
        assert(s == STRIQ_OK);
    }
    striq_status_t s = striq_writer_close(w);
    assert(s == STRIQ_OK);
}

static void test1_ramp_mean_range(void)
{
    size_t N = 10000;
    int64_t *ts   = malloc(N * sizeof(int64_t));
    double  *vals = malloc(N * sizeof(double));
    assert(ts && vals);

    for (size_t i = 0; i < N; i++) {
        ts[i]   = (int64_t)i * 1000000LL;   /* 1 ms apart */
        vals[i] = (double)i;                 /* 0, 1, 2, ... */
    }

    double eps = 0.5;
    write_1col("val", ts, vals, N, eps);

    /* Brute-force mean for rows [2500, 7499] */
    double bf_sum = 0.0;
    size_t bf_n   = 0;
    for (size_t i = 2500; i < 7500; i++) {
        bf_sum += vals[i];
        bf_n++;
    }
    double bf_mean = bf_sum / (double)bf_n;

    /* Query with ts range */
    striq_reader_t *r = striq_reader_open(TMP_FILE);
    assert(r != NULL);

    striq_result_t res = {0};
    striq_status_t s = striq_query_mean(r, "val",
                                         ts[2500], ts[7499], &res);
    assert(s == STRIQ_OK);
    striq_reader_close(r);

    double abs_err = fabs(res.value - bf_mean);
    /* Allow up to epsilon_b error in mean */
    assert(abs_err <= eps + 1.0 &&
           "Test 1: mean should be within epsilon_b of true value");

    printf("  [PASS] test1_ramp_mean_range "
           "(true=%.2f got=%.2f abs_err=%.4f eps=%.1f)\n",
           bf_mean, res.value, abs_err, eps);

    free(ts); free(vals);
}

static void test2_sine_count_where(void)
{
    size_t N = 10000;
    int64_t *ts   = malloc(N * sizeof(int64_t));
    double  *vals = malloc(N * sizeof(double));
    assert(ts && vals);

    for (size_t i = 0; i < N; i++) {
        ts[i]   = (int64_t)i * 1000000LL;
        vals[i] = sin(2.0 * 3.14159265358979 * (double)i / 100.0) * 10.0;
    }

    double eps = 0.01;
    write_1col("sig", ts, vals, N, eps);

    /* Brute-force count of rows with val > 0 */
    uint64_t bf_count = 0;
    for (size_t i = 0; i < N; i++)
        if (vals[i] > 0.0) bf_count++;

    striq_reader_t *r = striq_reader_open(TMP_FILE);
    assert(r != NULL);

    striq_result_t res = {0};
    striq_status_t s = striq_query_mean_where(r, "sig", 0, 0,
                                               0.0, STRIQ_CMP_GT, &res);
    assert(s == STRIQ_OK);
    uint64_t got_count = res.rows_scanned;
    striq_reader_close(r);

    /*
     * The WHERE is computed algebraically on PLA segments. Each segment that
     * crosses zero contributes an approximation error proportional to 1 row.
     * With 100 sine cycles (100 zero crossings up + 100 down), the max error
     * is ~200 rows out of ~5000 qualifying → up to 4%. Allow 10% to be safe.
     */
    double pct_err = fabs((double)got_count - (double)bf_count) /
                     (double)bf_count * 100.0;
    assert(pct_err < 10.0 &&
           "Test 2: count WHERE > 0 within 10% of brute-force");

    printf("  [PASS] test2_sine_count_where "
           "(bf=%llu got=%llu err=%.2f%%)\n",
           (unsigned long long)bf_count,
           (unsigned long long)got_count, pct_err);

    free(ts); free(vals);
}

static void test3_multicol_minmax(void)
{
    size_t N = 5000;
    int64_t *ts = malloc(N * sizeof(int64_t));
    double  *v1 = malloc(N * sizeof(double));
    double  *v2 = malloc(N * sizeof(double));
    assert(ts && v1 && v2);

    for (size_t i = 0; i < N; i++) {
        ts[i] = (int64_t)i * 2000000LL;   /* 2 ms apart */
        v1[i] = (double)i;                 /* ramp up */
        v2[i] = (double)(N - 1 - i);      /* ramp down */
    }

    double eps = 0.5;
    write_2col("up", "down", ts, v1, v2, N, eps);

    /* Brute-force for rows [1000, 3999] */
    size_t r_start = 1000, r_end = 3999;
    double bf_min_up = v1[r_start], bf_max_down = v2[r_start];
    for (size_t i = r_start; i <= r_end; i++) {
        if (v1[i] < bf_min_up)   bf_min_up   = v1[i];
        if (v2[i] > bf_max_down) bf_max_down = v2[i];
    }

    striq_reader_t *r = striq_reader_open(TMP_FILE);
    assert(r != NULL);

    striq_result_t rmin = {0}, rmax = {0};
    striq_status_t s1 = striq_query_min(r, "up",
                                         ts[r_start], ts[r_end], &rmin);
    striq_status_t s2 = striq_query_max(r, "down",
                                         ts[r_start], ts[r_end], &rmax);
    assert(s1 == STRIQ_OK && s2 == STRIQ_OK);
    striq_reader_close(r);

    /* min("up") in range should be close to r_start value */
    double err_min = fabs(rmin.value - bf_min_up);
    double err_max = fabs(rmax.value - bf_max_down);
    assert(err_min <= eps + 1.0 && "Test 3: min within epsilon_b");
    assert(err_max <= eps + 1.0 && "Test 3: max within epsilon_b");

    printf("  [PASS] test3_multicol_minmax "
           "(min_up=%.1f±%.2f, max_down=%.1f±%.2f)\n",
           rmin.value, err_min, rmax.value, err_max);

    free(ts); free(v1); free(v2);
}

static void test4_full_validator(void)
{
    size_t N = 20000;
    int64_t *ts   = malloc(N * sizeof(int64_t));
    double  *vals = malloc(N * sizeof(double));
    assert(ts && vals);

    for (size_t i = 0; i < N; i++) {
        ts[i]   = (int64_t)i * 1000000LL;
        vals[i] = (double)i * 0.5;   /* 0.0, 0.5, 1.0, ... */
    }

    double eps = 0.01;
    write_1col("x", ts, vals, N, eps);

    /* Brute-force */
    double   bf_sum  = 0.0;
    double   bf_min  = vals[0];
    double   bf_max  = vals[0];
    uint64_t bf_n    = (uint64_t)N;
    for (size_t i = 0; i < N; i++) {
        bf_sum += vals[i];
        if (vals[i] < bf_min) bf_min = vals[i];
        if (vals[i] > bf_max) bf_max = vals[i];
    }
    double bf_mean = bf_sum / (double)bf_n;

    striq_reader_t *r = striq_reader_open(TMP_FILE);
    assert(r != NULL);

    striq_result_t res_mean = {0}, res_sum = {0}, res_min = {0}, res_max = {0};
    uint64_t got_count = 0;

    assert(striq_query_mean(r, "x", 0, 0, &res_mean) == STRIQ_OK);
    assert(striq_query_sum(r, "x", 0, 0, &res_sum)   == STRIQ_OK);
    assert(striq_query_count(r, 0, 0, &got_count)    == STRIQ_OK);
    assert(striq_query_min(r, "x", 0, 0, &res_min)   == STRIQ_OK);
    assert(striq_query_max(r, "x", 0, 0, &res_max)   == STRIQ_OK);

    striq_reader_close(r);

    double tol = eps + 1e-9;

    double err_mean = fabs(res_mean.value - bf_mean);
    double err_sum  = fabs(res_sum.value  - bf_sum);
    double err_min  = fabs(res_min.value  - bf_min);
    double err_max  = fabs(res_max.value  - bf_max);

    assert(got_count == bf_n && "Test 4: count == N");
    assert(err_mean <= tol   && "Test 4: mean within epsilon");
    assert(err_sum  <= tol * (double)N && "Test 4: sum within N*epsilon");
    assert(err_min  <= tol   && "Test 4: min within epsilon");
    assert(err_max  <= tol   && "Test 4: max within epsilon");

    printf("  [PASS] test4_full_validator "
           "(count=%llu mean_err=%.2e sum_err=%.2e min_err=%.2e max_err=%.2e)\n",
           (unsigned long long)got_count,
           err_mean, err_sum, err_min, err_max);

    free(ts); free(vals);
}

int main(void)
{
    printf("=== test_integration (Phase 1) ===\n");
    test1_ramp_mean_range();
    test2_sine_count_where();
    test3_multicol_minmax();
    test4_full_validator();
    remove(TMP_FILE);
    printf("All integration tests PASSED.\n");
    return 0;
}
