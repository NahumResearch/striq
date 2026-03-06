#ifndef STRIQ_ROUTER_H
#define STRIQ_ROUTER_H

#include "../types.h"

/*
 * Trial-fit router: encodes the first 256 sample points with each candidate
 * codec and picks the one with the best bytes-per-point ratio.
 *
 * Decision cascade:
 *   1. unique_count ≤ 64              → CODEC_RLE
 *   2. PLA trial (LINEAR and CHEB)    → best bytes/point wins
 *   3. decimal_detect passes          → CODEC_DECIMAL
 *   4. ε > 0, range/65535 ≤ ε        → CODEC_QUANT16
 *   5. ε > 0, range/255   ≤ ε        → CODEC_QUANT8
 *   6. fallback                       → CODEC_RAW_STATS
 *
 * epsilon_b: the same error bound used for production encoding (≤0 = auto).
 */
striq_status_t router_select(
    const double  *values,
    size_t         n,
    double         epsilon_b,
    striq_codec_t *out_codec
);

#endif /* STRIQ_ROUTER_H */
