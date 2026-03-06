/* test_epsilon_invariant.c — |decoded[i] - original[i]| <= epsilon for all i. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <math.h>
#include "../lib/core/encoder.h"
#include "../lib/core/decoder.h"

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

typedef struct {
    int    n_violations;     /* points where |err| > ε */
    double max_err;          /* worst-case error observed */
    double avg_err;          /* average error */
    double comp_ratio;       /* raw / compressed */
    size_t comp_bytes;
    size_t raw_bytes;
} invariant_result_t;

static invariant_result_t check_epsilon(const char *label,
                                         double *orig, int N,
                                         double epsilon)
{
    static uint8_t mem[32 * 1024 * 1024];
    mctx_t wctx = { mem, sizeof(mem), 0, 0 };

    striq_col_schema_t cols[1] = {{ "v", COL_FLOAT64, CODEC_PLA_LINEAR, 0.0 }};
    static striq_encoder_t enc;
    striq_opts_t enc_opts = striq_opts_make(); enc_opts.epsilon = epsilon;
    assert(encoder_init(&enc, cols, 1, &enc_opts, mw, &wctx) == STRIQ_OK);
    for (int i = 0; i < N; i++) {
        int64_t ts = (int64_t)i;
        assert(encoder_add_row(&enc, ts, &orig[i], 1) == STRIQ_OK);
    }
    assert(encoder_close(&enc) == STRIQ_OK);

    mctx_t rctx = { mem, sizeof(mem), wctx.size, 0 };
    static striq_decoder_t dec;
    assert(decoder_init(&dec, mr, ms, &rctx, wctx.size) == STRIQ_OK);

    invariant_result_t res = {0};
    res.raw_bytes  = (size_t)N * sizeof(double);
    res.comp_bytes = wctx.size;
    res.comp_ratio = (double)res.raw_bytes / (double)res.comp_bytes;

    int row = 0;
    for (uint32_t bi = 0; bi < dec.fmt.num_blocks; bi++) {
        uint32_t n = dec.fmt.block_index[bi].num_rows;
        double *out = malloc(n * sizeof(double));
        assert(out);
        assert(decoder_read_column(&dec, bi, 0, out, n) == STRIQ_OK);

        for (uint32_t j = 0; j < n; j++) {
            double err = fabs(out[j] - orig[row]);
            res.avg_err += err;
            if (err > res.max_err) res.max_err = err;
            if (err > epsilon + 1e-9) res.n_violations++;
            row++;
        }
        free(out);
    }
    res.avg_err /= N;

    printf("  %-24s  ε=%-6.3f  violations=%d/%d  max_err=%.6f  "
           "avg_err=%.7f  ratio=%.1fx\n",
           label, epsilon,
           res.n_violations, N,
           res.max_err, res.avg_err, res.comp_ratio);

    return res;
}

#define N 3000

int main(void)
{
    printf("=== test_epsilon_invariant ===\n\n");

    double *vals = malloc((size_t)N * sizeof(double));
    assert(vals);

    double epsilons[] = { 0.001, 0.01, 0.1, 1.0 };
    int    ne = 4;

    printf("  -- linear ramp 0..100 --\n");
    for (int i = 0; i < N; i++) vals[i] = (double)i / (N-1) * 100.0;
    for (int ei = 0; ei < ne; ei++) {
        invariant_result_t r = check_epsilon("linear_ramp", vals, N, epsilons[ei]);
        assert(r.n_violations == 0 && "linear ramp: no violations allowed");
    }

    printf("\n  -- slow sine 22±5 --\n");
    for (int i = 0; i < N; i++) vals[i] = 22.0 + 5.0 * sin((double)i * 0.002);
    for (int ei = 0; ei < ne; ei++) {
        invariant_result_t r = check_epsilon("slow_sine", vals, N, epsilons[ei]);
        assert(r.n_violations == 0 && "slow sine: no violations allowed");
    }

    printf("\n  -- step/plateau+spike --\n");
    for (int i = 0; i < N; i++) {
        if (i >= 500 && i < 510) vals[i] = 100.0;  /* short spike */
        else                      vals[i] = 0.0;
    }
    for (int ei = 0; ei < ne; ei++) {
        invariant_result_t r = check_epsilon("step_spike", vals, N, epsilons[ei]);
        assert(r.n_violations == 0 && "step+spike: no violations allowed");
    }

    printf("\n  -- monotone decreasing --\n");
    for (int i = 0; i < N; i++) vals[i] = 1000.0 - (double)i * 0.5;
    for (int ei = 0; ei < ne; ei++) {
        invariant_result_t r = check_epsilon("monotone_dec", vals, N, epsilons[ei]);
        assert(r.n_violations == 0 && "monotone dec: no violations allowed");
    }

    printf("\n  -- zigzag ±50 --\n");
    for (int i = 0; i < N; i++) vals[i] = (i % 2 == 0) ? 50.0 : -50.0;
    for (int ei = 0; ei < ne; ei++) {
        invariant_result_t r = check_epsilon("zigzag", vals, N, epsilons[ei]);
        assert(r.n_violations == 0 && "zigzag: no violations allowed");
    }

    printf("\n  -- plateau + jump --\n");
    for (int i = 0; i < N; i++) {
        if (i < N / 2)  vals[i] = 10.0;
        else            vals[i] = 90.0;
    }
    for (int ei = 0; ei < ne; ei++) {
        invariant_result_t r = check_epsilon("plateau_jump", vals, N, epsilons[ei]);
        assert(r.n_violations == 0 && "plateau+jump: no violations allowed");
    }

    printf("\n  [PASS] ε-invariant holds for all signals and all ε values\n");
    printf("  Observation: higher ε → better compression, same correctness guarantee.\n");

    free(vals);
    return 0;
}
