#ifndef STRIQ_STATS_H
#define STRIQ_STATS_H

#include "../types.h"

/*
 * Compute lag-1 autocorrelation ρ ∈ [-1, 1].
 * Returns STRIQ_ERR_CODEC if variance is zero (constant series).
 * In that case *out_rho is set to 0.0 (treat as not correlated).
 */
striq_status_t stats_autocorr(
    const double *values,
    size_t        n,
    double       *out_rho
);

/*
 * Compute Shannon entropy H (bits) using 256 equal-width histogram bins.
 * Returns STRIQ_ERR_PARAM if n < 2.
 */
striq_status_t stats_entropy(
    const double *values,
    size_t        n,
    double       *out_H
);

/*
 * Compute normalized second-difference variance ("curvature"):
 *
 *   d2[i] = values[i+2] - 2*values[i+1] + values[i]
 *   curvature = Var(d2) / Var(values)
 *
 * Values close to 0 mean the signal is locally linear → good PLA candidate.
 * Values around 6 indicate white noise (d2 = sum of 3 independent samples).
 *
 * Threshold for PLA routing: curvature < 0.1
 *
 * Returns STRIQ_ERR_PARAM if n < 3.
 * If Var(values) == 0 (constant series), sets *out_cv = 0.0 and returns
 * STRIQ_ERR_CODEC (same convention as stats_autocorr for constant input).
 */
striq_status_t stats_curvature(
    const double *values,
    size_t        n,
    double       *out_cv
);

/*
 * Count the number of distinct values in values[0..n-1] using at most
 * `max_unique` slots. If more than max_unique distinct values are found,
 * stops early and returns max_unique + 1 (indicating "too many").
 *
 * Uses the first `sample` points only (pass n to check all).
 */
uint32_t stats_unique_count(
    const double *values,
    size_t        n,
    uint32_t      max_unique
);

#endif /* STRIQ_STATS_H */
