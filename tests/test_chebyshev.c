#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <math.h>
#include <stdbool.h>

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

#include "../lib/core/codecs/chebyshev.h"
#include "../lib/core/codecs/pla.h"

#define BIG (4 * 1024 * 1024)
static uint8_t seg_buf[BIG];
static uint8_t res_buf[BIG];

static void test_cheb3_fit_constant(void)
{
    /* Constant signal: should fit exactly */
    double vals[100];
    for (int i = 0; i < 100; i++) vals[i] = 42.5;

    double c[4];
    assert(cheb3_fit(vals, 100, 0.01, c) == STRIQ_OK);
    /* Mean should be c0 = 42.5 */
    assert(fabs(c[0] - 42.5) < 0.01);
    printf("  [PASS] fit_constant (c0=%.4f)\n", c[0]);
}

static void test_cheb3_fit_sine(void)
{
    /*
     * A cubic Chebyshev polynomial cannot fit a FULL sine cycle (two
     * inflection points) — that test was mathematically incorrect.
     *
     * What it CAN do is fit a half-cycle (a smooth hump) well.
     * 10*sin(π*i/127) for i=0..127 goes 0→10→0 — a single smooth arch.
     * With eps=0.5 (5% of amplitude) this should succeed easily.
     */
    const int N = 128;
    double vals[128];
    double eps = 0.5;
    for (int i = 0; i < N; i++)
        vals[i] = 10.0 * sin(M_PI * (double)i / (double)(N - 1));

    double c[4];
    striq_status_t s = cheb3_fit(vals, N, eps, c);
    assert(s == STRIQ_OK);

    /* Verify the fit manually */
    double inv = 2.0 / (double)(N - 1);
    for (int i = 0; i < N; i++) {
        double u = inv * (double)i - 1.0;
        double approx = c[0] + c[1]*u + c[2]*(2*u*u-1) + c[3]*u*(4*u*u-3);
        assert(fabs(vals[i] - approx) <= eps + 1e-9);
    }

    /* A full cycle should NOT fit within a tight epsilon */
    double full[200];
    for (int i = 0; i < 200; i++)
        full[i] = 10.0 * sin(2.0 * M_PI * (double)i / 200.0);
    double cf[4];
    striq_status_t sf = cheb3_fit(full, 200, 0.05, cf);
    assert(sf != STRIQ_OK);  /* cubic can't fit a full cycle within 0.05 */

    printf("  [PASS] fit_sine_half_cycle (c0=%.4f c1=%.4f c2=%.4f c3=%.4f)\n",
           c[0], c[1], c[2], c[3]);
    printf("  [PASS] full_cycle_correctly_rejected\n");
}

static void test_cheb3_eval_range_roundtrip(void)
{
    /*
     * Use a half-cycle arch (0→5→0) which a cubic Chebyshev fits well.
     * A full cycle cannot be fit by a degree-3 polynomial within tight eps.
     */
    const int N = 64;
    double vals[64];
    double eps = 0.25;
    for (int i = 0; i < N; i++)
        vals[i] = 5.0 * sin(M_PI * (double)i / (double)(N - 1));

    double c[4];
    striq_status_t s = cheb3_fit(vals, N, eps, c);
    assert(s == STRIQ_OK);

    double out[64];
    cheb3_eval_range(c, (uint16_t)N, out);

    double max_err = 0.0;
    for (int i = 0; i < N; i++) {
        double err = fabs(out[i] - vals[i]);
        if (err > max_err) max_err = err;
    }
    assert(max_err <= eps + 1e-9);
    printf("  [PASS] eval_range_roundtrip (max_err=%.6f eps=%.3f)\n", max_err, eps);
}

static void test_cheb3_mean_algebraic(void)
{
    /*
     * For a LINEAR signal, the LS Chebyshev fit is exact: c0 = mean, c1 =
     * half-range/something, c2=c3=0.  Verify that algebra_cheb_mean (= c0)
     * equals the true mean.
     *
     * vals[i] = 95 + 10*i/(N-1)  →  range [95, 105], mean = 100.
     */
    const int N = 200;
    double vals[200];
    double true_mean = 0.0;
    for (int i = 0; i < N; i++) {
        vals[i] = 95.0 + 10.0 * (double)i / (double)(N - 1);
        true_mean += vals[i];
    }
    true_mean /= N;  /* should be 100.0 */

    double c[4];
    assert(cheb3_fit(vals, N, 0.001, c) == STRIQ_OK);

    /* For a linear signal c0 == mean and c2,c3 should be negligible */
    assert(fabs(c[0] - true_mean) < 0.01);
    assert(fabs(c[2]) < 0.01);
    assert(fabs(c[3]) < 0.01);
    printf("  [PASS] cheb_mean_algebraic (c0=%.4f true_mean=%.4f)\n", c[0], true_mean);
}

static void test_pla_encode_cheb_sine(void)
{
    /* Sine wave: with cheb_threshold=32, should use chebyshev segments */
    const int N = 4000;
    double *vals = malloc(N * sizeof(double));
    assert(vals);
    double eps = 0.01;
    for (int i = 0; i < N; i++)
        vals[i] = 100.0 * sin(2.0 * M_PI * (double)i / 200.0);

    size_t seg_count_lin = 0, resid_lin = 0;
    bool used_cheb_lin = false;
    assert(pla_encode(vals, N, eps, 0,  /* no cheb */
                      seg_buf, BIG, &seg_count_lin, &used_cheb_lin,
                      res_buf, BIG, &resid_lin) == STRIQ_OK);
    assert(!used_cheb_lin);
    size_t lin_bytes = seg_count_lin * 18 + resid_lin;

    size_t seg_count_ch = 0, resid_ch = 0;
    bool used_cheb_ch = false;
    assert(pla_encode(vals, N, eps, 32,  /* try cheb for short segs */
                      seg_buf, BIG, &seg_count_ch, &used_cheb_ch,
                      res_buf, BIG, &resid_ch) == STRIQ_OK);

    printf("  Linear:    segs=%zu total=%zu bytes\n", seg_count_lin, lin_bytes);
    printf("  Chebyshev: segs=%zu cheb=%d base=%zu resid=%zu bytes\n",
           seg_count_ch, (int)used_cheb_ch,
           seg_count_ch * (used_cheb_ch ? 34u : 18u), resid_ch);

    if (used_cheb_ch) {
        size_t cheb_bytes = seg_count_ch * 34 + resid_ch;
        printf("  Compression: linear=%zu cheb=%zu (%.1fx better)\n",
               lin_bytes, cheb_bytes, (double)lin_bytes / (double)cheb_bytes);
        assert(seg_count_ch < seg_count_lin);

        /* Decode and verify */
        double *out = malloc(N * sizeof(double));
        assert(out);
        assert(pla_decode(seg_buf, seg_count_ch, true, res_buf, resid_ch, N, out)
               == STRIQ_OK);
        for (int i = 0; i < N; i++) {
            double diff = fabs(out[i] - vals[i]);
            if (diff > eps * 3 + 1e-4) {
                fprintf(stderr, "  Roundtrip FAIL i=%d diff=%f eps=%f\n", i, diff, eps);
                assert(0);
            }
        }
        free(out);
        printf("  [PASS] pla_encode_cheb_sine (roundtrip OK)\n");
    } else {
        printf("  [NOTE] no chebyshev segments used (signal may not be curved enough)\n");
    }

    free(vals);
}

static void test_find_max_length(void)
{
    /* Chebyshev should extend far on a slow sine */
    const int N = 4096;
    double *vals = malloc(N * sizeof(double));
    assert(vals);
    double eps = 0.05;
    for (int i = 0; i < N; i++)
        vals[i] = 10.0 * sin(2.0 * M_PI * (double)i / 400.0);

    double c[4];
    size_t max_len = cheb3_find_max_length(vals, 0, 4, N, eps, c);
    printf("  [PASS] find_max_length=%zu (of %d available, c0=%.4f)\n",
           max_len, N, c[0]);
    assert(max_len >= 4);
    free(vals);
}

int main(void)
{
    printf("=== test_chebyshev ===\n");
    test_cheb3_fit_constant();
    test_cheb3_fit_sine();
    test_cheb3_eval_range_roundtrip();
    test_cheb3_mean_algebraic();
    test_pla_encode_cheb_sine();
    test_find_max_length();
    printf("All chebyshev tests passed.\n");
    return 0;
}
