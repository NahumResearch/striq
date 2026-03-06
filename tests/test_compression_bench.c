/*
 * test_compression_bench.c — Verify compression ratios by signal type.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <math.h>
#include "../lib/core/encoder.h"

typedef struct { uint8_t *buf; size_t cap; size_t size; } wctx_t;
static striq_status_t mw(const uint8_t *d, size_t n, void *c)
{
    wctx_t *m = (wctx_t *)c;
    if (m->size + n > m->cap) return STRIQ_ERR_MEMORY;
    memcpy(m->buf + m->size, d, n);
    m->size += n;
    return STRIQ_OK;
}

typedef struct {
    const char *name;
    size_t      raw_bytes;
    size_t      striq_bytes;
    double      ratio;
} bench_t;

#define N 100000

static bench_t run_bench(const char *name,
                         double *vals, int64_t *ts,
                         int n_rows, double epsilon)
{
    static uint8_t striq_mem[64 * 1024 * 1024];
    wctx_t wctx = { striq_mem, sizeof(striq_mem), 0 };

    striq_col_schema_t cols[1] = {{ "v", COL_FLOAT64, CODEC_PLA_LINEAR, 0.0 }};
    static striq_encoder_t enc;
    striq_opts_t enc_opts = striq_opts_make(); enc_opts.epsilon = epsilon;
    assert(encoder_init(&enc, cols, 1, &enc_opts, mw, &wctx) == STRIQ_OK);
    for (int i = 0; i < n_rows; i++)
        assert(encoder_add_row(&enc, ts[i], &vals[i], 1) == STRIQ_OK);
    assert(encoder_close(&enc) == STRIQ_OK);

    size_t raw_bytes = (size_t)n_rows * sizeof(double);
    bench_t b;
    b.name        = name;
    b.raw_bytes   = raw_bytes;
    b.striq_bytes = wctx.size;
    b.ratio       = (double)raw_bytes / (double)wctx.size;
    return b;
}

int main(void)
{
    printf("=== test_compression_bench ===\n");
    printf("  N=%d rows\n\n", N);

    double  *vals = malloc((size_t)N * sizeof(double));
    int64_t *ts   = malloc((size_t)N * sizeof(int64_t));
    assert(vals && ts);
    for (int i = 0; i < N; i++) ts[i] = (int64_t)i * 1000000LL;

    bench_t results[5];
    int nr = 0;

    for (int i = 0; i < N; i++) vals[i] = (double)i / (N - 1) * 100.0;
    results[nr++] = run_bench("linear_ramp", vals, ts, N, 0.01);

    for (int i = 0; i < N; i++) vals[i] = 20.0 + 5.0 * sin((double)i * 0.0005);
    results[nr++] = run_bench("slow_sine", vals, ts, N, 0.01);

    srand(42);
    double temp = 22.0;
    for (int i = 0; i < N; i++) {
        temp += ((double)rand() / RAND_MAX - 0.5) * 0.05;
        if (temp < 20.0) temp = 20.0;
        if (temp > 26.0) temp = 26.0;
        vals[i] = round(temp * 10.0) / 10.0;
    }
    results[nr++] = run_bench("temperature_iot", vals, ts, N, 0.05);

    {
        double v = 0.0;
        int dwell = 500, cnt = 0;
        for (int i = 0; i < N; i++) {
            vals[i] = v;
            if (++cnt >= dwell) {
                v     = (v == 0.0) ? 100.0 : 0.0;
                cnt   = 0;
                dwell = 200 + rand() % 1800;
            }
        }
    }
    results[nr++] = run_bench("step_function", vals, ts, N, 0.01);

    srand(1337);
    for (int i = 0; i < N; i++) vals[i] = (double)rand() / RAND_MAX * 100.0;
    results[nr++] = run_bench("white_noise", vals, ts, N, 0.01);

    printf("  %-18s  %s\n", "signal", "striq_ratio");
    printf("  %s\n", "-------------------------------");
    for (int i = 0; i < nr; i++)
        printf("  %-18s  %5.1fx\n", results[i].name, results[i].ratio);

    assert(results[0].ratio > 3.0 && "linear_ramp must compress >3x");
    assert(results[1].ratio > 1.5 && "slow_sine must compress >1.5x");
    assert(results[2].ratio > 1.4 && "temperature_iot must compress >1.4x");
    assert(results[3].ratio > 2.0 && "step_function must compress >2x");
    assert(results[4].striq_bytes < results[4].raw_bytes * 2 &&
           "white_noise must not balloon beyond 2x raw");

    printf("\n  [PASS] all compression assertions\n");

    free(vals); free(ts);
    return 0;
}
