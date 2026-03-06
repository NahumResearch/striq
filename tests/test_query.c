#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <math.h>
#include "../lib/core/encoder.h"
#include "../lib/core/decoder.h"
#include "../lib/core/query/engine.h"
#include "../lib/adapters/file_provider.h"

typedef struct { uint8_t *buf; size_t cap; size_t size; size_t pos; } mctx_t;
static striq_status_t m_write(const uint8_t *d, size_t n, void *c)
{ mctx_t *m=(mctx_t*)c; if(m->size+n>m->cap) return STRIQ_ERR_MEMORY;
  memcpy(m->buf+m->size,d,n); m->size+=n; return STRIQ_OK; }
static striq_status_t m_read(uint8_t *b, size_t n, void *c)
{ mctx_t *m=(mctx_t*)c; if(m->pos+n>m->size) return STRIQ_ERR_IO;
  memcpy(b,m->buf+m->pos,n); m->pos+=n; return STRIQ_OK; }
static striq_status_t m_seek(int64_t off, int wh, void *c) {
    mctx_t *m = (mctx_t *)c;
    size_t np;
    if (wh == 0) np = (size_t)off;
    else if (wh == 1) np = m->pos + (size_t)off;
    else np = m->size + (size_t)off;
    if (np > m->size) return STRIQ_ERR_IO;
    m->pos = np;
    return STRIQ_OK;
}

static void test_mean_ramp(void)
{
    static uint8_t mem[16 * 1024 * 1024];
    mctx_t wctx = { mem, sizeof(mem), 0, 0 };

    striq_col_schema_t cols[1] = {{ "val", COL_FLOAT64, CODEC_PLA_LINEAR, 0.0 }};
    static striq_encoder_t enc;
    striq_opts_t enc_opts = striq_opts_make(); enc_opts.epsilon = 0.001;
    assert(encoder_init(&enc, cols, 1, &enc_opts, m_write, &wctx) == STRIQ_OK);

    /*
     * Staircase: 10 distinct values (0..9), 100 rows each → 1000 rows.
     * ρ ≈ 1, H ≈ 3.32 bits → routes to PLA.
     * Exact mean = (0*100 + 1*100 + … + 9*100) / 1000 = 4500/1000 = 4.5
     */
    for (int i = 0; i < 1000; i++) {
        int64_t ts = (int64_t)i;
        double v = (double)(i / 100);
        assert(encoder_add_row(&enc, ts, &v, 1) == STRIQ_OK);
    }
    assert(encoder_close(&enc) == STRIQ_OK);

    mctx_t rctx = { mem, sizeof(mem), wctx.size, 0 };
    static striq_fmt_reader_t fmt;
    assert(fmt_reader_open(&fmt, m_read, m_seek, &rctx, wctx.size) == STRIQ_OK);

    static striq_file_provider_t fp;
    assert(file_provider_init(&fp, &fmt) == STRIQ_OK);
    striq_query_engine_t qe;
    assert(engine_init(&qe, &fp.base) == STRIQ_OK);

    striq_query_result_t res = {0};
    assert(engine_query_mean(&qe, "val", 0, 0, &res) == STRIQ_OK);

    double expected = 4.5;
    assert(fabs(res.value - expected) <= res.error_bound + 1e-4);
    printf("  [PASS] mean_staircase (mean=%.2f expected=%.2f ±%.6f)\n",
           res.value, expected, res.error_bound);
}

static void test_count(void)
{
    static uint8_t mem2[4 * 1024 * 1024];
    mctx_t wctx = { mem2, sizeof(mem2), 0, 0 };

    striq_col_schema_t cols[1] = {{ "x", COL_FLOAT64, CODEC_PLA_LINEAR, 0.0 }};
    static striq_encoder_t enc2;
    striq_opts_t enc2_opts = striq_opts_make(); enc2_opts.epsilon = 0.01;
    assert(encoder_init(&enc2, cols, 1, &enc2_opts, m_write, &wctx) == STRIQ_OK);

    for (int i = 0; i < 500; i++) {
        int64_t ts = (int64_t)i;
        double v = (double)(i / 50); /* staircase: 10 distinct values */
        assert(encoder_add_row(&enc2, ts, &v, 1) == STRIQ_OK);
    }
    assert(encoder_close(&enc2) == STRIQ_OK);

    mctx_t rctx = { mem2, sizeof(mem2), wctx.size, 0 };
    static striq_fmt_reader_t fmt2;
    assert(fmt_reader_open(&fmt2, m_read, m_seek, &rctx, wctx.size) == STRIQ_OK);

    static striq_file_provider_t fp2;
    assert(file_provider_init(&fp2, &fmt2) == STRIQ_OK);
    striq_query_engine_t qe;
    assert(engine_init(&qe, &fp2.base) == STRIQ_OK);

    uint64_t cnt = 0;
    assert(engine_query_count(&qe, 0, 0, &cnt) == STRIQ_OK);
    assert(cnt == 500);
    printf("  [PASS] count (got %llu)\n", (unsigned long long)cnt);
}

static void test_downsample(void)
{
    /* Build a ramp: value = i, timestamps 0..N-1 ns */
    static uint8_t mem3[4 * 1024 * 1024];
    mctx_t wctx = { mem3, sizeof(mem3), 0, 0 };

    striq_col_schema_t cols[1] = {{ "v", COL_FLOAT64, CODEC_PLA_LINEAR, 0.0 }};
    static striq_encoder_t enc3;
    striq_opts_t enc3_opts = striq_opts_make(); enc3_opts.epsilon = 0.5;
    assert(encoder_init(&enc3, cols, 1, &enc3_opts, m_write, &wctx) == STRIQ_OK);

    uint32_t N = 1000;
    for (uint32_t i = 0; i < N; i++) {
        int64_t ts = (int64_t)i;
        double v   = (double)i;
        assert(encoder_add_row(&enc3, ts, &v, 1) == STRIQ_OK);
    }
    assert(encoder_close(&enc3) == STRIQ_OK);

    mctx_t rctx = { mem3, sizeof(mem3), wctx.size, 0 };
    static striq_fmt_reader_t fmt3;
    assert(fmt_reader_open(&fmt3, m_read, m_seek, &rctx, wctx.size) == STRIQ_OK);

    static striq_file_provider_t fp3;
    assert(file_provider_init(&fp3, &fmt3) == STRIQ_OK);
    striq_query_engine_t qe;
    assert(engine_init(&qe, &fp3.base) == STRIQ_OK);

    uint32_t n_pts = 11;
    double   vals[11];
    int64_t  tss[11];

    assert(engine_query_downsample(&qe, "v", 0, 0, n_pts, vals, tss) == STRIQ_OK);

    /* For a ramp y=t, the value at each sample ts should be ~ ts (within epsilon) */
    bool all_ok = true;
    for (uint32_t i = 0; i < n_pts; i++) {
        double expected = (double)tss[i];
        /* Allow PLA epsilon headroom */
        if (fabs(vals[i] - expected) > 2.0) {
            printf("  [FAIL] downsample point %u: ts=%lld val=%.4f expected=%.4f\n",
                   i, (long long)tss[i], vals[i], expected);
            all_ok = false;
        }
    }
    assert(all_ok);

    /* Test with explicit ts_from/ts_to */
    int64_t t0 = tss[0], t1 = tss[n_pts - 1];
    assert(engine_query_downsample(&qe, "v", t0, t1, n_pts, vals, tss) == STRIQ_OK);
    for (uint32_t i = 0; i < n_pts; i++)
        assert(!isnan(vals[i]));

    printf("  [PASS] downsample (n=%u first=%.2f last=%.2f)\n",
           n_pts, vals[0], vals[n_pts - 1]);
}

int main(void)
{
    printf("=== test_query ===\n");
    test_mean_ramp();
    test_count();
    test_downsample();
    printf("All query tests passed.\n");
    return 0;
}
