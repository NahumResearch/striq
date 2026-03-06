#include <stdio.h>
#include <assert.h>
#include <math.h>
#include <stdlib.h>
#include "../lib/core/routing/stats.h"

static void test_autocorr_ramp(void)
{
    double vals[100];
    for (int i = 0; i < 100; i++) vals[i] = (double)i;
    double rho = 0.0;
    striq_status_t s = stats_autocorr(vals, 100, &rho);
    assert(s == STRIQ_OK);
    assert(rho > 0.95); /* perfect linear → ρ close to 1 (exact value depends on n) */
    printf("  [PASS] autocorr_ramp (rho=%.4f)\n", rho);
}

static void test_autocorr_constant(void)
{
    double vals[50];
    for (int i = 0; i < 50; i++) vals[i] = 5.0;
    double rho = 0.0;
    striq_status_t s = stats_autocorr(vals, 50, &rho);
    /* constant → variance=0 → STRIQ_ERR_CODEC, rho forced to 0 */
    assert(s == STRIQ_ERR_CODEC);
    assert(rho == 0.0);
    printf("  [PASS] autocorr_constant\n");
}

static void test_autocorr_single(void)
{
    double v = 1.0;
    double rho = 0.0;
    striq_status_t s = stats_autocorr(&v, 1, &rho);
    assert(s == STRIQ_ERR_CODEC);
    assert(rho == 0.0);
    printf("  [PASS] autocorr_single\n");
}

static void test_entropy_uniform(void)
{
    /* Values uniformly spread across range → high entropy */
    double vals[256];
    for (int i = 0; i < 256; i++) vals[i] = (double)i;
    double H = 0.0;
    assert(stats_entropy(vals, 256, &H) == STRIQ_OK);
    assert(H > 7.9); /* ≈ log2(256) = 8 */
    printf("  [PASS] entropy_uniform (H=%.4f)\n", H);
}

static void test_entropy_constant(void)
{
    double vals[50];
    for (int i = 0; i < 50; i++) vals[i] = 3.14;
    double H = 0.0;
    assert(stats_entropy(vals, 50, &H) == STRIQ_OK);
    assert(H == 0.0);
    printf("  [PASS] entropy_constant (H=0.0)\n");
}

static void test_curvature_linear_ramp(void)
{
    /* Perfect linear signal: d2 = 0 exactly → curvature = 0 */
    double vals[200];
    for (int i = 0; i < 200; i++) vals[i] = 3.0 * i + 7.0;
    double cv = -1.0;
    striq_status_t s = stats_curvature(vals, 200, &cv);
    assert(s == STRIQ_OK);
    assert(cv < 1e-10 && "linear ramp curvature must be ~0");
    printf("  [PASS] curvature_linear_ramp (cv=%.2e)\n", cv);
}

static void test_curvature_slow_sine(void)
{
    /*
     * Slow sine: ω = 0.001 rad/sample, amplitude 50.
     * d2 ≈ A*ω²*sin → Var(d2) ≈ A²*ω⁴/2 ; Var(values) = A²/2
     * → curvature ≈ ω⁴ ≈ (0.001)⁴ = 1e-12 << 0.1
     */
    double vals[500];
    for (int i = 0; i < 500; i++) vals[i] = 50.0 * sin((double)i * 0.001);
    double cv = -1.0;
    striq_status_t s = stats_curvature(vals, 500, &cv);
    assert(s == STRIQ_OK);
    assert(cv < 0.001 && "slow sine must have very low curvature");
    printf("  [PASS] curvature_slow_sine (cv=%.2e)\n", cv);
}

static void test_curvature_white_noise(void)
{
    /* White noise: d2 is sum of 3 independent values → Var(d2) ≈ 6*Var(values) */
    double vals[1000];
    srand(4242);
    for (int i = 0; i < 1000; i++) vals[i] = (double)rand() / RAND_MAX;
    double cv = -1.0;
    striq_status_t s = stats_curvature(vals, 1000, &cv);
    assert(s == STRIQ_OK);
    assert(cv > 1.0 && "white noise curvature must be >> 0.1");
    printf("  [PASS] curvature_white_noise (cv=%.4f)\n", cv);
}

static void test_curvature_constant(void)
{
    /* Constant: variance = 0 → returns STRIQ_ERR_CODEC, cv = 0 */
    double vals[50];
    for (int i = 0; i < 50; i++) vals[i] = 42.0;
    double cv = -1.0;
    striq_status_t s = stats_curvature(vals, 50, &cv);
    assert(s == STRIQ_ERR_CODEC);
    assert(cv == 0.0);
    printf("  [PASS] curvature_constant\n");
}

int main(void)
{
    printf("=== test_stats ===\n");
    test_autocorr_ramp();
    test_autocorr_constant();
    test_autocorr_single();
    test_entropy_uniform();
    test_entropy_constant();
    test_curvature_linear_ramp();
    test_curvature_slow_sine();
    test_curvature_white_noise();
    test_curvature_constant();
    printf("All stats tests passed.\n");
    return 0;
}
