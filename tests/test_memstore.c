#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <math.h>
#include <stdint.h>
#include "../lib/core/encoder.h"
#include "../lib/core/format/writer.h"
#include "../lib/core/format/reader.h"
#include "../lib/core/query/engine.h"
#include "../lib/adapters/memstore.h"

typedef struct { uint8_t *buf; size_t cap; size_t size; size_t pos; } mctx_t;

static striq_status_t m_write(const uint8_t *d, size_t n, void *c)
{
    mctx_t *m = (mctx_t *)c;
    if (m->size + n > m->cap) return STRIQ_ERR_MEMORY;
    memcpy(m->buf + m->size, d, n);
    m->size += n;
    return STRIQ_OK;
}
static striq_status_t m_read(uint8_t *b, size_t n, void *c)
{
    mctx_t *m = (mctx_t *)c;
    if (m->pos + n > m->size) return STRIQ_ERR_IO;
    memcpy(b, m->buf + m->pos, n);
    m->pos += n;
    return STRIQ_OK;
}
static striq_status_t m_seek(int64_t off, int wh, void *c)
{
    mctx_t *m = (mctx_t *)c;
    size_t np;
    if (wh == 0)      np = (size_t)off;
    else if (wh == 1) np = m->pos + (size_t)off;
    else              np = m->size + (size_t)off;
    if (np > m->size) return STRIQ_ERR_IO;
    m->pos = np;
    return STRIQ_OK;
}

/*
 * Encode N rows into a tmp buffer, then load all blocks from that
 * buffer and push them into the given memstore.
 *
 * Returns brute-force sum and count for validation.
 */
static void encode_and_push(
    striq_memstore_t *ms,
    const char *col_name, double *vals, int64_t *ts, uint32_t n,
    double epsilon_b,
    double *out_bf_sum, uint32_t *out_bf_n)
{
    static uint8_t tmp[16 * 1024 * 1024];
    mctx_t wctx = { tmp, sizeof(tmp), 0, 0 };

    striq_col_schema_t cols[1];
    memset(cols, 0, sizeof(cols));
    snprintf(cols[0].name, sizeof(cols[0].name), "%s", col_name);
    cols[0].type      = STRIQ_COL_FLOAT64;
    cols[0].codec     = CODEC_PLA_LINEAR;
    cols[0].epsilon_b = 0.0;

    static striq_encoder_t enc;
    striq_opts_t enc_opts = striq_opts_make(); enc_opts.epsilon = epsilon_b;
    assert(encoder_init(&enc, cols, 1, &enc_opts, m_write, &wctx) == STRIQ_OK);
    for (uint32_t i = 0; i < n; i++)
        assert(encoder_add_row(&enc, ts[i], &vals[i], 1) == STRIQ_OK);
    assert(encoder_close(&enc) == STRIQ_OK);

    /* Brute-force stats */
    double bf_sum = 0.0;
    for (uint32_t i = 0; i < n; i++) bf_sum += vals[i];
    *out_bf_sum = bf_sum;
    *out_bf_n   = n;

    /* Open as fmt_reader to extract blocks */
    mctx_t rctx = { tmp, sizeof(tmp), wctx.size, 0 };
    static striq_fmt_reader_t fmt;
    assert(fmt_reader_open(&fmt, m_read, m_seek, &rctx, wctx.size) == STRIQ_OK);

    /* Push each block into memstore */
    for (uint32_t bi = 0; bi < fmt.num_blocks; bi++) {
        uint32_t bsz = fmt.block_index[bi].block_size;
        uint8_t *blk = malloc(bsz);
        assert(blk);
        size_t rlen = 0;
        assert(fmt_reader_read_block_raw(&fmt, bi, blk, bsz, &rlen) == STRIQ_OK);
        assert(memstore_push_block(ms, blk, (uint32_t)rlen,
                                   &fmt.block_index[bi],
                                   fmt.block_stats[bi]) == STRIQ_OK);
        free(blk);
    }
}

static void test_push_and_query(void)
{
    uint32_t N = 5000;
    double  *vals = malloc(N * sizeof(double));
    int64_t *ts   = malloc(N * sizeof(int64_t));
    assert(vals && ts);
    for (uint32_t i = 0; i < N; i++) {
        vals[i] = (double)i * 0.5;
        ts[i]   = (int64_t)i * 1000000LL;
    }

    striq_col_schema_t cols[1] = {{ "v", STRIQ_COL_FLOAT64, CODEC_PLA_LINEAR, 0.0 }};
    striq_memstore_opts_t opts = { .epsilon_b = 0.001, .max_blocks = 64 };
    striq_memstore_t *ms = memstore_create(cols, 1, &opts);
    assert(ms);

    double bf_sum; uint32_t bf_n;
    encode_and_push(ms, "v", vals, ts, N, 0.001, &bf_sum, &bf_n);

    striq_query_engine_t qe;
    assert(engine_init(&qe, memstore_provider(ms)) == STRIQ_OK);

    striq_query_result_t res = {0};
    assert(engine_query_mean(&qe, "v", 0, 0, &res) == STRIQ_OK);

    double true_mean = bf_sum / bf_n;
    double abs_err   = fabs(res.value - true_mean);
    assert(abs_err <= res.error_bound + 1e-4);
    printf("  [PASS] push_and_query (mean=%.4f expected=%.4f err=%.6f)\n",
           res.value, true_mean, abs_err);

    striq_memstore_info_t info;
    assert(memstore_info(ms, &info) == STRIQ_OK);
    assert(info.num_blocks > 0);
    assert(info.total_bytes > 0);
    assert(info.ts_oldest == ts[0]);

    free(vals); free(ts);
    memstore_destroy(ms);
}

static int eviction_count = 0;
static void eviction_cb(
    const uint8_t *data, uint32_t size,
    const striq_block_index_t *idx,
    const striq_col_stats_t *stats, uint32_t num_cols, void *user)
{
    (void)data; (void)size; (void)idx; (void)stats; (void)num_cols; (void)user;
    eviction_count++;
}

static void test_eviction(void)
{
    eviction_count = 0;
    uint32_t N = 10000;  /* needs > ENCODER_MAX_ROWS_PER_BLOCK (4096) * max_blocks (2) */
    double  *vals = malloc(N * sizeof(double));
    int64_t *ts   = malloc(N * sizeof(int64_t));
    assert(vals && ts);

    for (uint32_t i = 0; i < N; i++) {
        vals[i] = (double)(i % 100);
        ts[i]   = (int64_t)i * 1000000LL;
    }

    /* Set max_blocks = 2 so eviction happens quickly */
    striq_col_schema_t cols[1] = {{ "x", STRIQ_COL_FLOAT64, CODEC_PLA_LINEAR, 0.0 }};
    striq_memstore_opts_t opts = {
        .epsilon_b    = 0.01,
        .max_blocks   = 2,
        .eviction_cb  = eviction_cb,
        .eviction_ctx = NULL
    };
    striq_memstore_t *ms = memstore_create(cols, 1, &opts);
    assert(ms);

    double bf_sum; uint32_t bf_n;
    encode_and_push(ms, "x", vals, ts, N, 0.01, &bf_sum, &bf_n);

    /* Ring should be capped at 2 blocks */
    striq_memstore_info_t info;
    assert(memstore_info(ms, &info) == STRIQ_OK);
    assert(info.num_blocks <= 2);
    assert(eviction_count > 0);

    printf("  [PASS] eviction (evicted=%d, remaining=%u blocks)\n",
           eviction_count, info.num_blocks);

    free(vals); free(ts);
    memstore_destroy(ms);
}

static void test_memory_cap(void)
{
    uint32_t N = 4096;
    double  *vals = malloc(N * sizeof(double));
    int64_t *ts   = malloc(N * sizeof(int64_t));
    assert(vals && ts);
    for (uint32_t i = 0; i < N; i++) {
        vals[i] = sin((double)i * 0.01) * 100.0;
        ts[i]   = (int64_t)i * 1000000LL;
    }

    striq_col_schema_t cols[1] = {{ "s", STRIQ_COL_FLOAT64, CODEC_PLA_LINEAR, 0.0 }};
    /* 1 MB memory cap */
    striq_memstore_opts_t opts = { .max_memory_mb = 1 };
    striq_memstore_t *ms = memstore_create(cols, 1, &opts);
    assert(ms);

    double bf_sum; uint32_t bf_n;
    encode_and_push(ms, "s", vals, ts, N, 0.01, &bf_sum, &bf_n);

    striq_memstore_info_t info;
    assert(memstore_info(ms, &info) == STRIQ_OK);
    assert(info.total_bytes <= 1024u * 1024u);

    printf("  [PASS] memory_cap (bytes=%llu <= 1MB, blocks=%u)\n",
           (unsigned long long)info.total_bytes, info.num_blocks);

    free(vals); free(ts);
    memstore_destroy(ms);
}

static void test_ts_ordering(void)
{
    uint32_t N = 1000;
    double  *vals = malloc(N * sizeof(double));
    int64_t *ts   = malloc(N * sizeof(int64_t));
    assert(vals && ts);
    for (uint32_t i = 0; i < N; i++) {
        vals[i] = (double)i;
        ts[i]   = (int64_t)i * 2000000LL;
    }

    striq_col_schema_t cols[1] = {{ "t", STRIQ_COL_FLOAT64, CODEC_PLA_LINEAR, 0.0 }};
    striq_memstore_opts_t opts = { .max_blocks = 16 };
    striq_memstore_t *ms = memstore_create(cols, 1, &opts);
    assert(ms);

    double bf_sum; uint32_t bf_n;
    encode_and_push(ms, "t", vals, ts, N, 0.1, &bf_sum, &bf_n);

    striq_memstore_info_t info;
    assert(memstore_info(ms, &info) == STRIQ_OK);
    assert(info.ts_oldest <= info.ts_newest);
    assert(info.ts_oldest >= ts[0]);

    printf("  [PASS] ts_ordering (oldest=%lld newest=%lld)\n",
           (long long)info.ts_oldest, (long long)info.ts_newest);

    free(vals); free(ts);
    memstore_destroy(ms);
}

static void test_count_query(void)
{
    uint32_t N = 2000;
    double  *vals = malloc(N * sizeof(double));
    int64_t *ts   = malloc(N * sizeof(int64_t));
    assert(vals && ts);
    for (uint32_t i = 0; i < N; i++) {
        vals[i] = (double)(i * 3);
        ts[i]   = (int64_t)i * 1000000LL;
    }

    striq_col_schema_t cols[1] = {{ "c", STRIQ_COL_FLOAT64, CODEC_PLA_LINEAR, 0.0 }};
    striq_memstore_opts_t opts = { .max_blocks = 32 };
    striq_memstore_t *ms = memstore_create(cols, 1, &opts);
    assert(ms);

    double bf_sum; uint32_t bf_n;
    encode_and_push(ms, "c", vals, ts, N, 0.001, &bf_sum, &bf_n);

    striq_query_engine_t qe;
    assert(engine_init(&qe, memstore_provider(ms)) == STRIQ_OK);

    uint64_t cnt = 0;
    assert(engine_query_count(&qe, 0, 0, &cnt) == STRIQ_OK);
    assert(cnt == N);
    printf("  [PASS] count_query (count=%llu expected=%u)\n",
           (unsigned long long)cnt, N);

    free(vals); free(ts);
    memstore_destroy(ms);
}

int main(void)
{
    printf("=== test_memstore ===\n");
    test_push_and_query();
    test_eviction();
    test_memory_cap();
    test_ts_ordering();
    test_count_query();
    printf("All memstore tests passed.\n");
    return 0;
}
