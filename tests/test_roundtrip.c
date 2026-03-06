#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <math.h>
#include "../lib/core/encoder.h"
#include "../lib/core/decoder.h"

typedef struct { uint8_t *buf; size_t cap; size_t size; size_t pos; } mctx_t;
static striq_status_t mw(const uint8_t *d,size_t n,void *c){mctx_t *m=(mctx_t*)c;if(m->size+n>m->cap)return STRIQ_ERR_MEMORY;memcpy(m->buf+m->size,d,n);m->size+=n;return STRIQ_OK;}
static striq_status_t mr(uint8_t *b,size_t n,void *c){mctx_t *m=(mctx_t*)c;if(m->pos+n>m->size)return STRIQ_ERR_IO;memcpy(b,m->buf+m->pos,n);m->pos+=n;return STRIQ_OK;}
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

static void test_roundtrip_pla(void)
{
    static uint8_t mem[32 * 1024 * 1024];
    mctx_t wctx = { mem, sizeof(mem), 0, 0 };

    int N = 5000;
    double eps = 0.001;

    /*
     * Slow staircase: 10 distinct integer values, each repeated N/10 times.
     * ρ ≈ 1, H ≈ 3.32 bits → routes to PLA → strong compression + exact decode.
     */
    double *orig_vals = malloc((size_t)N * sizeof(double));
    int64_t *orig_ts  = malloc((size_t)N * sizeof(int64_t));
    assert(orig_vals && orig_ts);
    for (int i = 0; i < N; i++) {
        orig_ts[i]   = (int64_t)i * 1000000LL;
        orig_vals[i] = (double)(i / (N / 10));
    }

    /* Encode */
    striq_col_schema_t cols[1] = {{ "signal", COL_FLOAT64, CODEC_PLA_LINEAR, 0.0 }};
    static striq_encoder_t enc;
    striq_opts_t enc_opts = striq_opts_make(); enc_opts.epsilon = eps;
    assert(encoder_init(&enc, cols, 1, &enc_opts, mw, &wctx) == STRIQ_OK);
    for (int i = 0; i < N; i++)
        assert(encoder_add_row(&enc, orig_ts[i], &orig_vals[i], 1) == STRIQ_OK);
    assert(encoder_close(&enc) == STRIQ_OK);

    /* Decode */
    mctx_t rctx = { mem, sizeof(mem), wctx.size, 0 };
    static striq_decoder_t dec;
    assert(decoder_init(&dec, mr, ms, &rctx, wctx.size) == STRIQ_OK);

    uint32_t nb = dec.fmt.num_blocks;
    assert(nb > 0);

    int verified = 0;
    for (uint32_t bi = 0; bi < nb; bi++) {
        uint32_t n = dec.fmt.block_index[bi].num_rows;
        double  *vals_out = malloc(n * sizeof(double));
        int64_t *ts_out   = malloc(n * sizeof(int64_t));
        assert(vals_out && ts_out);

        assert(decoder_read_timestamps(&dec, bi, ts_out, n) == STRIQ_OK);
        assert(decoder_read_column(&dec, bi, 0, vals_out, n) == STRIQ_OK);

        for (uint32_t j = 0; j < n; j++) {
            assert(ts_out[j] == orig_ts[verified]);
            double diff = fabs(vals_out[j] - orig_vals[verified]);
            if (diff > eps * 2.0 + 1e-9) {
                fprintf(stderr, "FAIL row %d: got %.9f expected %.9f diff %.9f\n",
                        verified, vals_out[j], orig_vals[verified], diff);
                assert(0);
            }
            verified++;
        }
        free(vals_out); free(ts_out);
    }
    assert(verified == N);

    free(orig_vals); free(orig_ts);
    printf("  [PASS] roundtrip_pla (%d rows, %zu bytes, ratio=%.1fx)\n",
           N, wctx.size, (double)(N * sizeof(double)) / (double)wctx.size);
}

static void test_roundtrip_noise(void)
{
    static uint8_t mem[32 * 1024 * 1024];
    mctx_t wctx = { mem, sizeof(mem), 0, 0 };

    int N = 1000;
    double epsilon = 0.001;

    /*
     * Random noise with ε=0.001.
     * Phase 7: routes to QUANT16 (range≈1, range/65535≈1.5e-5 < ε=0.001),
     * giving 4x compression with bounded error.  Exact equality is NOT
     * guaranteed when ε > 0; we verify |err| ≤ ε instead.
     */
    double *orig_vals = malloc((size_t)N * sizeof(double));
    int64_t *orig_ts  = malloc((size_t)N * sizeof(int64_t));
    assert(orig_vals && orig_ts);
    srand(999);
    for (int i = 0; i < N; i++) {
        orig_ts[i]   = (int64_t)i;
        orig_vals[i] = (double)rand() / (double)RAND_MAX;
    }

    striq_col_schema_t cols[1] = {{ "noise", COL_FLOAT64, CODEC_PLA_LINEAR, 0.0 }};
    static striq_encoder_t enc2;
    striq_opts_t enc2_opts = striq_opts_make(); enc2_opts.epsilon = epsilon;
    assert(encoder_init(&enc2, cols, 1, &enc2_opts, mw, &wctx) == STRIQ_OK);
    for (int i = 0; i < N; i++)
        assert(encoder_add_row(&enc2, orig_ts[i], &orig_vals[i], 1) == STRIQ_OK);
    assert(encoder_close(&enc2) == STRIQ_OK);

    mctx_t rctx = { mem, sizeof(mem), wctx.size, 0 };
    static striq_decoder_t dec2;
    assert(decoder_init(&dec2, mr, ms, &rctx, wctx.size) == STRIQ_OK);

    int verified = 0;
    for (uint32_t bi = 0; bi < dec2.fmt.num_blocks; bi++) {
        uint32_t n = dec2.fmt.block_index[bi].num_rows;
        double *out = malloc(n * sizeof(double));
        assert(out);
        assert(decoder_read_column(&dec2, bi, 0, out, n) == STRIQ_OK);
        for (uint32_t j = 0; j < n; j++) {
            double err = out[j] - orig_vals[verified];
            if (err < 0) err = -err;
            assert(err <= epsilon + 1e-9);
            verified++;
        }
        free(out);
    }
    assert(verified == N);

    free(orig_vals); free(orig_ts);
    printf("  [PASS] roundtrip_noise (%d rows, within ε=%.3f)\n", N, epsilon);
}

int main(void)
{
    printf("=== test_roundtrip ===\n");
    test_roundtrip_pla();
    test_roundtrip_noise();
    printf("All roundtrip tests passed.\n");
    return 0;
}
