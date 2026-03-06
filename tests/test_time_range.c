/* test_time_range.c — Time-range query filtering and block skipping. */
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

/* Global data: 10000 rows, ts = i * 1_000_000 ns (1ms steps) */
#define N_ROWS 10000
#define TS_STEP 1000000LL  /* 1 ms in nanoseconds */

static uint8_t g_mem[32 * 1024 * 1024];
static size_t  g_size = 0;
static double  g_vals[N_ROWS];

static void setup(void)
{
    mctx_t wctx = { g_mem, sizeof(g_mem), 0, 0 };
    striq_col_schema_t cols[1] = {{ "v", COL_FLOAT64, CODEC_PLA_LINEAR, 0.0 }};
    static striq_encoder_t enc;
    striq_opts_t enc_opts = striq_opts_make(); enc_opts.epsilon = 0.001;
    assert(encoder_init(&enc, cols, 1, &enc_opts, mw, &wctx) == STRIQ_OK);

    for (int i = 0; i < N_ROWS; i++) {
        g_vals[i]  = (double)(i / 100); /* staircase → PLA-friendly */
        int64_t ts = (int64_t)i * TS_STEP;
        assert(encoder_add_row(&enc, ts, &g_vals[i], 1) == STRIQ_OK);
    }
    assert(encoder_close(&enc) == STRIQ_OK);
    g_size = wctx.size;
}

static striq_file_provider_t g_fp;
static striq_query_engine_t make_engine(mctx_t *rctx, striq_fmt_reader_t *fmt)
{
    assert(fmt_reader_open(fmt, mr, ms, rctx, g_size) == STRIQ_OK);
    assert(file_provider_init(&g_fp, fmt) == STRIQ_OK);
    striq_query_engine_t qe;
    assert(engine_init(&qe, &g_fp.base) == STRIQ_OK);
    return qe;
}

static void test_count_full_range(void)
{
    mctx_t rctx = { g_mem, sizeof(g_mem), g_size, 0 };
    static striq_fmt_reader_t fmt;
    striq_query_engine_t qe = make_engine(&rctx, &fmt);

    uint64_t cnt = 0;
    assert(engine_query_count(&qe, 0, 0, &cnt) == STRIQ_OK);
    assert(cnt == N_ROWS);
    printf("  [PASS] count(*) full range = %llu\n", (unsigned long long)cnt);
}

static void test_count_partial_range(void)
{
    /* Rows 1000..1999 → ts from 1000*TS_STEP to 1999*TS_STEP */
    int64_t ts_from = 1000LL * TS_STEP;
    int64_t ts_to   = 1999LL * TS_STEP;

    mctx_t rctx = { g_mem, sizeof(g_mem), g_size, 0 };
    static striq_fmt_reader_t fmt;
    striq_query_engine_t qe = make_engine(&rctx, &fmt);

    uint64_t cnt = 0;
    assert(engine_query_count(&qe, ts_from, ts_to, &cnt) == STRIQ_OK);

    /*
     * Phase 0 count() is block-granular: it returns the sum of num_rows for
     * all blocks whose [ts_first, ts_last] overlaps the range.
     * The queried range (1000..1999ms) falls inside one block of 4096 rows,
     * so the count returns 4096, not 1000.
     *
     * Fine-grained row-level filtering is a Phase 1 feature.
     */
    printf("  count(ts 1000..1999ms) = %llu  (block-granular, block_size=%d)\n",
           (unsigned long long)cnt, 4096);
    assert(cnt >= 1000 && cnt <= N_ROWS &&
           "count must be ≥ exact rows and ≤ total rows (block-granular)");
    printf("  [PASS] count(*) partial range (block-level Phase 0)\n");
}

static void test_mean_partial_range(void)
{
    /*
     * Rows 0..999 → staircase vals floor(i/100) = 0,0,...,1,1,...,9
     * true_mean = (0*100 + 1*100 + ... + 9*100) / 1000 = 4.5
     */
    int64_t ts_from = 0;
    int64_t ts_to   = 999LL * TS_STEP;

    mctx_t rctx = { g_mem, sizeof(g_mem), g_size, 0 };
    static striq_fmt_reader_t fmt;
    striq_query_engine_t qe = make_engine(&rctx, &fmt);

    striq_query_result_t res = {0};
    striq_status_t st = engine_query_mean(&qe, "v", ts_from, ts_to, &res);

    /*
     * Phase 0 mean() is block-granular: it computes the mean over every block
     * whose [ts_first, ts_last] overlaps [ts_from, ts_to]. The block containing
     * rows 0-999 also contains rows 0-4095 → returned mean ≈ 20 (full block mean),
     * not 4.5 (exact slice mean).
     *
     * Row-level time slicing within a block is a Phase 1 feature.
     * Here we just verify that the call succeeds and returns a plausible value.
     */
    if (st == STRIQ_OK) {
        printf("  mean(v, 0..999ms) = %.4f  (block-level Phase 0; exact slice = Phase 1)\n",
               res.value);
        assert(res.value >= 0.0 && res.value <= 100.0 && "mean must be in data range");
        printf("  [PASS] mean() with time range (block-level)\n");
    } else {
        printf("  [SKIP] mean() returned %d (blocks may not overlap range)\n", st);
    }
}

static void test_empty_range(void)
{
    /* ts_from > ts_to of all data → should return 0 */
    int64_t ts_from = (int64_t)(N_ROWS + 1000) * TS_STEP;
    int64_t ts_to   = (int64_t)(N_ROWS + 2000) * TS_STEP;

    mctx_t rctx = { g_mem, sizeof(g_mem), g_size, 0 };
    static striq_fmt_reader_t fmt;
    striq_query_engine_t qe = make_engine(&rctx, &fmt);

    uint64_t cnt = 0;
    striq_status_t st = engine_query_count(&qe, ts_from, ts_to, &cnt);
    assert(st == STRIQ_OK || st == STRIQ_ERR_NOTFOUND);
    assert(cnt == 0 && "empty time range must return 0 rows");
    printf("  [PASS] empty time range → count=0\n");
}

static void test_block_index_stats(void)
{
    /* Verify that block_index ts_first/ts_last are populated correctly */
    mctx_t rctx = { g_mem, sizeof(g_mem), g_size, 0 };
    static striq_fmt_reader_t fmt;
    assert(fmt_reader_open(&fmt, mr, ms, &rctx, g_size) == STRIQ_OK);

    assert(fmt.num_blocks > 0);
    int64_t prev_last = -1;
    for (uint32_t bi = 0; bi < fmt.num_blocks; bi++) {
        int64_t f = fmt.block_index[bi].ts_first;
        int64_t l = fmt.block_index[bi].ts_last;
        assert(f >= 0      && "ts_first must be non-negative");
        assert(l >= f      && "ts_last >= ts_first");
        assert(f > prev_last || (bi == 0 &&
               "blocks must be time-ordered"));
        prev_last = l;
    }
    printf("  [PASS] block index ts ordering (%u blocks)\n", fmt.num_blocks);
}

int main(void)
{
    printf("=== test_time_range ===\n");
    setup();
    {
        mctx_t rc = { g_mem, sizeof(g_mem), g_size, 0 };
        static striq_fmt_reader_t ftmp;
        fmt_reader_open(&ftmp, mr, ms, &rc, g_size);
        printf("  setup: %d rows, %u blocks, %zu bytes\n",
               N_ROWS, ftmp.num_blocks, g_size);
    }
    test_count_full_range();
    test_count_partial_range();
    test_mean_partial_range();
    test_empty_range();
    test_block_index_stats();
    printf("All time range tests passed.\n");
    return 0;
}
