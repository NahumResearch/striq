/* test_multicolumn.c — Multi-column encode/decode independence. */
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

/* Ground truth generators — each uses a distinct formula */
static double gen_temp    (int i) { return 22.0 + (double)(i / 100) * 0.1; }
static double gen_humidity(int i) { return 60.0 + (double)(i / 200) * 0.5; }
static double gen_pressure(int i) { return 1013.0 + (double)(i / 50) * 0.2; }

#define N_ROWS 5000
#define N_COLS 3

static void test_3col_encode_decode(void)
{
    static uint8_t mem[32 * 1024 * 1024];
    mctx_t wctx = { mem, sizeof(mem), 0, 0 };

    striq_col_schema_t cols[N_COLS] = {
        { "temperature", COL_FLOAT64, CODEC_PLA_LINEAR, 0.0 },
        { "humidity",    COL_FLOAT64, CODEC_PLA_LINEAR, 0.0 },
        { "pressure",    COL_FLOAT64, CODEC_PLA_LINEAR, 0.0 },
    };
    static striq_encoder_t enc;
    striq_opts_t enc_opts = striq_opts_make(); enc_opts.epsilon = 0.01;
    assert(encoder_init(&enc, cols, N_COLS, &enc_opts, mw, &wctx) == STRIQ_OK);

    for (int i = 0; i < N_ROWS; i++) {
        int64_t ts = (int64_t)i * 1000000LL;
        double row[N_COLS] = {
            gen_temp(i),
            gen_humidity(i),
            gen_pressure(i),
        };
        assert(encoder_add_row(&enc, ts, row, N_COLS) == STRIQ_OK);
    }
    assert(encoder_close(&enc) == STRIQ_OK);

    printf("  encoded: %zu bytes (vs raw %d bytes, %.1fx)\n",
           wctx.size, N_ROWS * (int)(N_COLS * sizeof(double) + sizeof(int64_t)),
           (double)(N_ROWS * (N_COLS * sizeof(double) + sizeof(int64_t))) / wctx.size);

    /* Decode and verify each column independently */
    mctx_t rctx = { mem, sizeof(mem), wctx.size, 0 };
    static striq_decoder_t dec;
    assert(decoder_init(&dec, mr, ms, &rctx, wctx.size) == STRIQ_OK);

    uint32_t total_rows = 0;
    for (uint32_t bi = 0; bi < dec.fmt.num_blocks; bi++)
        total_rows += dec.fmt.block_index[bi].num_rows;
    assert(total_rows == N_ROWS);

    /* Allocate decode buffers */
    double  *out_temp = malloc((size_t)N_ROWS * sizeof(double));
    double  *out_hum  = malloc((size_t)N_ROWS * sizeof(double));
    double  *out_pres = malloc((size_t)N_ROWS * sizeof(double));
    int64_t *out_ts   = malloc((size_t)N_ROWS * sizeof(int64_t));
    assert(out_temp && out_hum && out_pres && out_ts);

    int row = 0;
    for (uint32_t bi = 0; bi < dec.fmt.num_blocks; bi++) {
        uint32_t n = dec.fmt.block_index[bi].num_rows;
        assert(decoder_read_timestamps(&dec, bi, out_ts   + row, n) == STRIQ_OK);
        assert(decoder_read_column    (&dec, bi, 0, out_temp + row, n) == STRIQ_OK);
        assert(decoder_read_column    (&dec, bi, 1, out_hum  + row, n) == STRIQ_OK);
        assert(decoder_read_column    (&dec, bi, 2, out_pres + row, n) == STRIQ_OK);
        row += (int)n;
    }
    assert(row == N_ROWS);

    /* Verify every row */
    int ts_errors = 0, temp_errors = 0, hum_errors = 0, pres_errors = 0;
    double eps = 0.01;
    for (int i = 0; i < N_ROWS; i++) {
        if (out_ts[i] != (int64_t)i * 1000000LL) ts_errors++;
        if (fabs(out_temp[i] - gen_temp(i))     > eps + 1e-9) temp_errors++;
        if (fabs(out_hum[i]  - gen_humidity(i)) > eps + 1e-9) hum_errors++;
        if (fabs(out_pres[i] - gen_pressure(i)) > eps + 1e-9) pres_errors++;
    }

    printf("  ts_errors=%d  temp_errors=%d  hum_errors=%d  pres_errors=%d\n",
           ts_errors, temp_errors, hum_errors, pres_errors);

    assert(ts_errors   == 0 && "timestamps must be exact");
    assert(temp_errors == 0 && "temperature must be within epsilon");
    assert(hum_errors  == 0 && "humidity must be within epsilon");
    assert(pres_errors == 0 && "pressure must be within epsilon");

    printf("  [PASS] 3-column encode/decode (%d rows)\n", N_ROWS);
    free(out_temp); free(out_hum); free(out_pres); free(out_ts);
}

static void test_col_independence(void)
{
    /*
     * Encode 3 columns where each has a VERY different range.
     * If column offsets are wrong, decoded values will be from wrong column.
     * Any confusion would produce values far outside expected range.
     */
    static uint8_t mem[16 * 1024 * 1024];
    mctx_t wctx = { mem, sizeof(mem), 0, 0 };

    int N = 2000;
    striq_col_schema_t cols[3] = {
        { "small",  COL_FLOAT64, CODEC_PLA_LINEAR, 0.0 },
        { "medium", COL_FLOAT64, CODEC_PLA_LINEAR, 0.0 },
        { "large",  COL_FLOAT64, CODEC_PLA_LINEAR, 0.0 },
    };
    static striq_encoder_t enc2;
    striq_opts_t enc2_opts = striq_opts_make(); enc2_opts.epsilon = 0.01;
    assert(encoder_init(&enc2, cols, 3, &enc2_opts, mw, &wctx) == STRIQ_OK);

    for (int i = 0; i < N; i++) {
        int64_t ts = (int64_t)i;
        double row[3] = {
            (double)i / N,              /* [0, 1] */
            1000.0 + (double)i / N,     /* [1000, 1001] */
            1e6 + (double)i / N,        /* [1e6, 1e6+1] */
        };
        assert(encoder_add_row(&enc2, ts, row, 3) == STRIQ_OK);
    }
    assert(encoder_close(&enc2) == STRIQ_OK);

    mctx_t rctx = { mem, sizeof(mem), wctx.size, 0 };
    static striq_decoder_t dec2;
    assert(decoder_init(&dec2, mr, ms, &rctx, wctx.size) == STRIQ_OK);

    double *s = malloc((size_t)N * sizeof(double));
    double *m2 = malloc((size_t)N * sizeof(double));
    double *l = malloc((size_t)N * sizeof(double));
    assert(s && m2 && l);

    int row = 0;
    for (uint32_t bi = 0; bi < dec2.fmt.num_blocks; bi++) {
        uint32_t n = dec2.fmt.block_index[bi].num_rows;
        assert(decoder_read_column(&dec2, bi, 0, s  + row, n) == STRIQ_OK);
        assert(decoder_read_column(&dec2, bi, 1, m2 + row, n) == STRIQ_OK);
        assert(decoder_read_column(&dec2, bi, 2, l  + row, n) == STRIQ_OK);
        row += (int)n;
    }

    /* If columns got mixed up, values would be wildly out of range */
    for (int i = 0; i < N; i++) {
        assert(s[i]  >= -0.1    && s[i]  <= 1.1      && "small range corrupted");
        assert(m2[i] >= 999.0   && m2[i] <= 2001.0   && "medium range corrupted");
        assert(l[i]  >= 999999.0 && l[i] <= 2000001.0 && "large range corrupted");
    }

    printf("  [PASS] column independence (3 disjoint ranges)\n");
    free(s); free(m2); free(l);
}

int main(void)
{
    printf("=== test_multicolumn ===\n");
    test_3col_encode_decode();
    test_col_independence();
    printf("All multicolumn tests passed.\n");
    return 0;
}
