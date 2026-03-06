#include "router.h"
#include "stats.h"
#include "../codecs/pla.h"
#include "../codecs/decimal.h"
#include <stdbool.h>

/*
 * Trial-fit router (Phase 7).
 *
 * All exits have algebraic query support via a 48-byte stats header — zero exceptions.
 *
 *   1. unique_count ≤ 64              → CODEC_RLE       (algebraic ✅)
 *   2. Trial LINEAR + CHEB, compare bytes/point:
 *        both pass → pick lower bytes/point (algebraic ✅)
 *        only one passes → use it
 *   3. decimal_detect(sample)         → CODEC_DECIMAL    (lossless ✅, algebraic ✅)
 *   4. ε > 0, (max-min)/65535 ≤ ε    → CODEC_QUANT16   (4× lossy, algebraic ✅)
 *   5. ε > 0, (max-min)/255   ≤ ε    → CODEC_QUANT8    (8× lossy, algebraic ✅)
 *   6. fallback                        → CODEC_RAW_STATS (algebraic ✅)
 */

#define RLE_UNIQUE_THRESHOLD    64u    /* Phase 6: raised from 16 */
#define PLA_AVG_LINEAR          32.0   /* min avg_seg_len for PLA_LINEAR */
#define PLA_AVG_CHEB            16.0   /* min avg_seg_len for PLA_CHEB   */

/* Trial fit uses exactly TRIAL_N points so stack buffers are always safe. */
#define TRIAL_N        256u
#define TRIAL_SEG_BUF  9000  /* 256 × 34-byte Cheb segs + margin */
#define TRIAL_RES_BUF  2048  /* 256 × residual overhead + margin  */

striq_status_t router_select(
    const double  *values,
    size_t         n,
    double         epsilon_b,
    striq_codec_t *out_codec)
{
    if (!values || !out_codec) return STRIQ_ERR_PARAM;

    size_t sample = (n > STRIQ_ROUTE_SAMPLE) ? STRIQ_ROUTE_SAMPLE : n;

    uint32_t nu = stats_unique_count(values, sample, RLE_UNIQUE_THRESHOLD);
    if (nu <= RLE_UNIQUE_THRESHOLD) {
        *out_codec = CODEC_RLE;
        return STRIQ_OK;
    }

    size_t trial_n = (sample > TRIAL_N) ? TRIAL_N : sample;

    uint8_t seg_scratch[TRIAL_SEG_BUF];
    uint8_t res_scratch[TRIAL_RES_BUF];
    bool    used_cheb = false;

    size_t lin_count = 0, lin_res = 0;
    striq_status_t sp = pla_encode(
        values, trial_n, epsilon_b, 0,
        seg_scratch, sizeof(seg_scratch), &lin_count,
        &used_cheb,
        res_scratch, sizeof(res_scratch), &lin_res);

    size_t cheb_count = 0, cheb_res = 0;
    used_cheb = false;
    striq_status_t sc = pla_encode(
        values, trial_n, epsilon_b, 65535u,
        seg_scratch, sizeof(seg_scratch), &cheb_count,
        &used_cheb,
        res_scratch, sizeof(res_scratch), &cheb_res);

    bool linear_ok = (sp == STRIQ_OK && lin_count  > 0 &&
                      (double)trial_n / (double)lin_count  >= PLA_AVG_LINEAR);
    bool cheb_ok   = (sc == STRIQ_OK && cheb_count > 0 &&
                      (double)trial_n / (double)cheb_count >= PLA_AVG_CHEB);

    if (linear_ok || cheb_ok) {
        if (linear_ok && cheb_ok) {
            double lin_bpp  = 18.0 * (double)lin_count  / (double)trial_n;
            double cheb_bpp = 34.0 * (double)cheb_count / (double)trial_n;
            *out_codec = (cheb_bpp < lin_bpp) ? CODEC_PLA_CHEB : CODEC_PLA_LINEAR;
        } else if (cheb_ok) {
            *out_codec = CODEC_PLA_CHEB;
        } else {
            *out_codec = CODEC_PLA_LINEAR;
        }
        return STRIQ_OK;
    }

    uint8_t d_exp, delta_bytes;
    if (decimal_detect(values, (uint32_t)trial_n, &d_exp, &delta_bytes)) {
        *out_codec = CODEC_DECIMAL;
        return STRIQ_OK;
    }

    if (epsilon_b > 0.0) {
        double mn = values[0], mx = values[0];
        for (size_t i = 1; i < trial_n; i++) {
            if (values[i] < mn) mn = values[i];
            if (values[i] > mx) mx = values[i];
        }
        double range = mx - mn;
        if (range / 65535.0 <= epsilon_b) {
            *out_codec = CODEC_QUANT16;
            return STRIQ_OK;
        }
        if (range / 255.0 <= epsilon_b) {
            *out_codec = CODEC_QUANT8;
            return STRIQ_OK;
        }
    }

    *out_codec = CODEC_RAW_STATS;
    return STRIQ_OK;
}
