#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <math.h>
#include <stdbool.h>
#include "../lib/core/codecs/pla.h"

#define N 1000
#define BIG (4 * 1024 * 1024)

static uint8_t seg_buf[BIG];
static uint8_t res_buf[BIG];

static void check_roundtrip(const double *vals, size_t n, double eps, const char *label)
{
    size_t seg_count = 0, resid_len = 0;
    bool used_cheb = false;
    assert(pla_encode(vals, n, eps, 0, seg_buf, BIG, &seg_count, &used_cheb, res_buf, BIG, &resid_len) == STRIQ_OK);

    double *out = malloc(n * sizeof(double));
    assert(out);
    assert(pla_decode(seg_buf, seg_count, used_cheb, res_buf, resid_len, n, out) == STRIQ_OK);

    for (size_t i = 0; i < n; i++) {
        double diff = fabs(out[i] - vals[i]);
        if (diff > eps * 2.0 + 1e-9) {
            fprintf(stderr, "  FAIL %s i=%zu diff=%.9f eps=%.9f\n", label, i, diff, eps);
            assert(0);
        }
    }
    free(out);
    printf("  [PASS] %s (segs=%zu resid=%zu bytes cheb=%d)\n", label, seg_count, resid_len, (int)used_cheb);
}

static void test_constant(void)
{
    double vals[N];
    for (int i = 0; i < N; i++) vals[i] = 42.5;
    check_roundtrip(vals, N, 0.1, "constant");
}

static void test_ramp(void)
{
    double vals[N];
    for (int i = 0; i < N; i++) vals[i] = (double)i * 0.5;
    check_roundtrip(vals, N, 0.001, "ramp");
}

static void test_sine(void)
{
    double vals[N];
    for (int i = 0; i < N; i++) vals[i] = sin((double)i * 0.1);
    check_roundtrip(vals, N, 0.01, "sine");
}

static void test_noise(void)
{
    double vals[N];
    srand(42);
    for (int i = 0; i < N; i++) vals[i] = (double)rand() / RAND_MAX * 100.0;
    check_roundtrip(vals, N, 1.0, "noise");
}

static void test_single_point(void)
{
    double v = 3.14;
    check_roundtrip(&v, 1, 0.001, "single_point");
}

static void test_two_points(void)
{
    double v[2] = {1.0, 2.0};
    check_roundtrip(v, 2, 0.001, "two_points");
}

static void test_mean_query(void)
{
    /* Ramp: mean should be ~(N-1)/2 * 0.5 */
    double vals[N];
    for (int i = 0; i < N; i++) vals[i] = (double)i * 0.5;

    size_t seg_count = 0, resid_len = 0;
    bool   used_cheb = false;
    double eps = 0.001;
    assert(pla_encode(vals, N, eps, 0, seg_buf, BIG, &seg_count, &used_cheb, res_buf, BIG, &resid_len) == STRIQ_OK);

    double mean = 0.0, err = 0.0;
    assert(pla_query_mean(seg_buf, seg_count, eps, &mean, &err) == STRIQ_OK);

    double expected = (double)(N - 1) * 0.5 / 2.0;
    assert(fabs(mean - expected) <= eps + 1e-6);
    printf("  [PASS] mean_query (mean=%.4f expected=%.4f err=%.6f)\n", mean, expected, err);
}

int main(void)
{
    printf("=== test_pla ===\n");
    test_constant();
    test_ramp();
    test_sine();
    test_noise();
    test_single_point();
    test_two_points();
    test_mean_query();
    printf("All PLA tests passed.\n");
    return 0;
}
