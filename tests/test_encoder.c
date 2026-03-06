#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <math.h>
#include "../lib/core/encoder.h"

typedef struct { uint8_t *buf; size_t cap; size_t size; } wctx_t;
static striq_status_t mem_write(const uint8_t *d, size_t n, void *ctx)
{
    wctx_t *m = (wctx_t *)ctx;
    if (m->size + n > m->cap) return STRIQ_ERR_MEMORY;
    memcpy(m->buf + m->size, d, n);
    m->size += n;
    return STRIQ_OK;
}

static void test_100k_values(void)
{
    static uint8_t mem[32 * 1024 * 1024];
    wctx_t wctx = { mem, sizeof(mem), 0 };

    striq_col_schema_t cols[1] = {{ "temp", COL_FLOAT64, CODEC_PLA_LINEAR, 0.0 }};
    static striq_encoder_t enc;
    striq_opts_t enc_opts = striq_opts_make(); enc_opts.epsilon = 0.01;
    assert(encoder_init(&enc, cols, 1, &enc_opts, mem_write, &wctx) == STRIQ_OK);

    /*
     * Staircase with step 100: ~41 distinct values per 4096-row block.
     * H ≈ 5.3 bits << 6.5, ρ ≈ 0.98 → routes to PLA → good compression.
     */
    for (int i = 0; i < 100000; i++) {
        int64_t ts = (int64_t)i * 1000000LL;
        double v = (double)(i / 100);
        assert(encoder_add_row(&enc, ts, &v, 1) == STRIQ_OK);
    }
    assert(encoder_close(&enc) == STRIQ_OK);

    assert(wctx.size > 0);
    assert(wctx.size < 100000 * sizeof(double)); /* should compress */
    printf("  [PASS] 100K values: %zu bytes (vs raw %zu, ratio=%.1fx)\n",
           wctx.size, 100000 * sizeof(double),
           (double)(100000 * sizeof(double)) / (double)wctx.size);
}

int main(void)
{
    printf("=== test_encoder ===\n");
    test_100k_values();
    printf("All encoder tests passed.\n");
    return 0;
}
