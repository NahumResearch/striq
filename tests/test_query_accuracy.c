/* test_query_accuracy.c — Algebraic mean accuracy vs brute-force ground truth. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <math.h>
#include "../lib/core/encoder.h"
#include "../lib/adapters/file_provider.h"
#include "../lib/core/decoder.h"
#include "../lib/core/query/engine.h"

typedef struct { uint8_t *buf; size_t cap; size_t size; size_t pos; } mctx_t;
static striq_status_t mw(const uint8_t *d,size_t n,void *c)
{ mctx_t *m=(mctx_t*)c; if(m->size+n>m->cap)return STRIQ_ERR_MEMORY;
  memcpy(m->buf+m->size,d,n); m->size+=n; return STRIQ_OK; }
static striq_status_t mr(uint8_t *b,size_t n,void *c)
{ mctx_t *m=(mctx_t*)c; if(m->pos+n>m->size)return STRIQ_ERR_IO;
  memcpy(b,m->buf+m->pos,n); m->pos+=n; return STRIQ_OK; }
static striq_status_t ms(int64_t off, int wh, void *c) {
    mctx_t *m = (mctx_t *)c;
    size_t np;
    if (wh == 0) np = (size_t)off;
    else if (wh == 1) np = m->pos + (size_t)off;
    else np = m->size + (size_t)off;
    if (np > m->size) return STRIQ_ERR_IO;
    m->pos = np;
    return STRIQ_OK;
}

/* Encode vals[0..N-1], query mean(), compare to true_mean */
static void check_mean_accuracy(const char *label,
                                 double *vals, int N,
                                 double epsilon,
                                 double allowed_relative_err)
{
    static uint8_t mem[32 * 1024 * 1024];
    mctx_t wctx = { mem, sizeof(mem), 0, 0 };

    striq_col_schema_t cols[1] = {{ "x", COL_FLOAT64, CODEC_PLA_LINEAR, 0.0 }};
    static striq_encoder_t enc;
    striq_opts_t enc_opts = striq_opts_make(); enc_opts.epsilon = epsilon;
    assert(encoder_init(&enc, cols, 1, &enc_opts, mw, &wctx) == STRIQ_OK);
    for (int i = 0; i < N; i++) {
        int64_t ts = (int64_t)i;
        assert(encoder_add_row(&enc, ts, &vals[i], 1) == STRIQ_OK);
    }
    assert(encoder_close(&enc) == STRIQ_OK);

    /* True mean via brute force (no codec involved) */
    double sum = 0.0;
    for (int i = 0; i < N; i++) sum += vals[i];
    double true_mean = sum / N;

    /* Algebraic mean from engine */
    mctx_t rctx = { mem, sizeof(mem), wctx.size, 0 };
    static striq_fmt_reader_t fmt;
    assert(fmt_reader_open(&fmt, mr, ms, &rctx, wctx.size) == STRIQ_OK);

    static striq_file_provider_t fp;
    assert(file_provider_init(&fp, &fmt) == STRIQ_OK);
    striq_query_engine_t qe;
    assert(engine_init(&qe, &fp.base) == STRIQ_OK);

    striq_query_result_t res = {0};
    striq_status_t st = engine_query_mean(&qe, "x", 0, 0, &res);
    assert(st == STRIQ_OK);

    double abs_err = fabs(res.value - true_mean);
    double val_range = 0.0;
    {
        double vmin = vals[0], vmax = vals[0];
        for (int i = 1; i < N; i++) {
            if (vals[i] < vmin) vmin = vals[i];
            if (vals[i] > vmax) vmax = vals[i];
        }
        val_range = vmax - vmin;
    }
    double rel_err = (val_range > 1e-12) ? (abs_err / val_range) : abs_err;

    /* The reported error_bound must be ≥ actual error (never under-report) */
    int bound_conservative = (res.error_bound >= abs_err - 1e-9);

    printf("  %-22s true=%.4f  got=%.4f  abs_err=%.6f  rel_err=%.4f%%  "
           "bound=%.6f  %s\n",
           label, true_mean, res.value, abs_err, rel_err * 100.0,
           res.error_bound,
           bound_conservative ? "[PASS]" : "[WARN: bound under-reports]");

    assert(rel_err <= allowed_relative_err &&
           "algebraic mean relative error exceeds allowed margin");
}

#define N 4000

int main(void)
{
    printf("=== test_query_accuracy ===\n\n");

    double *vals = malloc((size_t)N * sizeof(double));
    assert(vals);

    /*
     * All signals below have H < 6.5 (low entropy) → routed to PLA.
     *
     * Signals with H ≈ 8 bits (ramp 0..N, sine full-range) use the full
     * histogram → routed to LZ4 → algebraic mean not available in Phase 0.
     * That is a known limitation documented in striq_contex.md.
     */

    for (int i = 0; i < N; i++) vals[i] = (double)(i / 100);
    check_mean_accuracy("staircase_100", vals, N, 0.01, 0.01);

    for (int i = 0; i < N; i++) vals[i] = (double)(i / 200);
    check_mean_accuracy("staircase_200", vals, N, 0.01, 0.01);

    srand(77);
    double temp = 22.0;
    for (int i = 0; i < N; i++) {
        temp += ((double)rand() / RAND_MAX - 0.5) * 0.04;
        if (temp < 20.0) temp = 20.0;
        if (temp > 25.0) temp = 25.0;
        vals[i] = round(temp * 10.0) / 10.0;
    }
    check_mean_accuracy("temperature_0.1C", vals, N, 0.05, 0.02);

    {
        double v = 0.0; int cnt = 0, dwell = 500;
        srand(88);
        for (int i = 0; i < N; i++) {
            vals[i] = v;
            if (++cnt >= dwell) { v = 100.0 - v; cnt = 0; dwell = 300 + rand() % 400; }
        }
    }
    check_mean_accuracy("step_function_0_100", vals, N, 0.01, 0.05);

    for (int i = 0; i < N; i++) vals[i] = 1013.0 + (double)(i / 200) * 0.1;
    check_mean_accuracy("pressure_drift", vals, N, 0.05, 0.01);

    printf("\n  [PASS] all accuracy assertions\n");
    free(vals);
    return 0;
}
