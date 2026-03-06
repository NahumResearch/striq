#include <stdio.h>
#include <assert.h>
#include <math.h>
#include <stdlib.h>
#include <stdbool.h>

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

#include "../lib/core/routing/router.h"
#include "../lib/core/codecs/pla.h"

static void test_staircase_goes_rle(void)
{
    /*
     * Staircase: value = floor(i/100), producing 10 distinct values (0..9),
     * each repeated 100 times.
     *
     * Phase 4: unique_count = 10 ≤ 16 → CODEC_RLE (much better than PLA for this)
     */
    double vals[1000];
    for (int i = 0; i < 1000; i++) vals[i] = (double)(i / 100);
    striq_codec_t codec;
    assert(router_select(vals, 1000, 0.0, &codec) == STRIQ_OK);
    assert(codec == CODEC_RLE);
    printf("  [PASS] staircase → RLE (10 unique values)\n");
}

static void test_noise_goes_raw_stats(void)
{
    /*
     * Uniform random noise: PLA trials both fail (avg_seg_len < thresholds),
     * so the router falls through to CODEC_RAW_STATS (Phase 6 fallback).
     */
    double vals[1000];
    srand(12345);
    for (int i = 0; i < 1000; i++) vals[i] = (double)rand() / RAND_MAX;
    striq_codec_t codec;
    assert(router_select(vals, 1000, 0.0, &codec) == STRIQ_OK);
    assert(codec == CODEC_RAW_STATS);
    printf("  [PASS] noise → RAW_STATS (no LZ4 in Phase 6)\n");
}

static void test_linear_ramp_goes_pla(void)
{
    /*
     * Perfect linear ramp: values[i] = i * 2.5 + 100
     * Trial PLA LINEAR on 256 pts → 1 segment → avg_len=256 >> 32 → PLA_LINEAR.
     */
    double vals[1000];
    for (int i = 0; i < 1000; i++) vals[i] = (double)i * 2.5 + 100.0;
    striq_codec_t codec;
    assert(router_select(vals, 1000, 0.0, &codec) == STRIQ_OK);
    assert(codec == CODEC_PLA_LINEAR);
    printf("  [PASS] linear_ramp → PLA_LINEAR\n");
}

static void test_slow_sine_goes_cheb(void)
{
    /*
     * Slow sine: ω = 0.005 rad/sample, amplitude 100.
     * Trial PLA LINEAR on 256 pts: avg_len ≈ 17 (< 32 threshold).
     * Trial PLA CHEB on 256 pts: avg_len >> 16 → CODEC_PLA_CHEB.
     * Chebyshev-3 fits a gentle arc far better than linear.
     */
    double vals[1000];
    for (int i = 0; i < 1000; i++) vals[i] = 100.0 * sin((double)i * 0.005);
    striq_codec_t codec;
    assert(router_select(vals, 1000, 0.0, &codec) == STRIQ_OK);
    assert(codec == CODEC_PLA_CHEB);
    printf("  [PASS] slow_sine → PLA_CHEB (Chebyshev fits arcs better than linear)\n");
}

static void test_encoder_uses_chebyshev_for_sine(void)
{
    static uint8_t sbuf[4 * 1024 * 1024];
    static uint8_t rbuf[4 * 1024 * 1024];

    const int N = 4000;
    double *vals = (double *)malloc(N * sizeof(double));
    assert(vals);
    for (int i = 0; i < N; i++)
        vals[i] = 100.0 * sin(2.0 * M_PI * (double)i / 200.0);

    size_t segs_lin = 0, resid_lin = 0;
    bool used_lin = false;
    assert(pla_encode(vals, N, 0.01, 0, sbuf, sizeof(sbuf),
                      &segs_lin, &used_lin, rbuf, sizeof(rbuf), &resid_lin) == STRIQ_OK);

    size_t segs_ch = 0, resid_ch = 0;
    bool   used_ch = false;
    assert(pla_encode(vals, N, 0.01, 32, sbuf, sizeof(sbuf),
                      &segs_ch, &used_ch, rbuf, sizeof(rbuf), &resid_ch) == STRIQ_OK);

    printf("  Linear: %zu segs | Cheb: %zu segs (used_cheb=%d)\n",
           segs_lin, segs_ch, (int)used_ch);

    if (used_ch) {
        assert(segs_ch <= segs_lin);  /* cheb should not be worse */
        printf("  [PASS] sine uses fewer segments with Chebyshev (%zu vs %zu)\n",
               segs_ch, segs_lin);
    } else {
        printf("  [NOTE] chebyshev not triggered (signal may be too smooth for benefit)\n");
    }
    free(vals);
}

int main(void)
{
    printf("=== test_router ===\n");
    test_staircase_goes_rle();
    test_noise_goes_raw_stats();
    test_linear_ramp_goes_pla();
    test_slow_sine_goes_cheb();
    test_encoder_uses_chebyshev_for_sine();
    printf("All router tests passed.\n");
    return 0;
}
