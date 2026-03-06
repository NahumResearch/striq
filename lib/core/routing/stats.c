#include "stats.h"
#include <math.h>
#include <string.h>
#include <stdint.h>

striq_status_t stats_autocorr(
    const double *values,
    size_t        n,
    double       *out_rho)
{
    if (!values || !out_rho) return STRIQ_ERR_PARAM;
    if (n < 2) { *out_rho = 0.0; return STRIQ_ERR_CODEC; }

    double mu = 0.0;
    for (size_t i = 0; i < n; i++) mu += values[i];
    mu /= (double)n;

    /* Variance (denominator) and lag-1 covariance (numerator) */
    double denom = 0.0, numer = 0.0;
    for (size_t i = 0; i < n - 1; i++) {
        double a = values[i]     - mu;
        double b = values[i + 1] - mu;
        numer += a * b;
        denom += a * a;
    }
    /* Include last point in denominator */
    double last = values[n - 1] - mu;
    denom += last * last;

    if (denom < 1e-15) {
        *out_rho = 0.0;
        return STRIQ_ERR_CODEC; /* constant series */
    }

    *out_rho = numer / denom;
    return STRIQ_OK;
}

striq_status_t stats_entropy(
    const double *values,
    size_t        n,
    double       *out_H)
{
    if (!values || !out_H) return STRIQ_ERR_PARAM;
    if (n < 2) return STRIQ_ERR_PARAM;

    double mn = values[0], mx = values[0];
    for (size_t i = 1; i < n; i++) {
        if (values[i] < mn) mn = values[i];
        if (values[i] > mx) mx = values[i];
    }

    double range = mx - mn;
    uint32_t hist[256];
    memset(hist, 0, sizeof(hist));

    if (range < 1e-15) {
        /* All values identical → 0 entropy */
        *out_H = 0.0;
        return STRIQ_OK;
    }

    double scale = 255.0 / range;
    for (size_t i = 0; i < n; i++) {
        int bin = (int)((values[i] - mn) * scale);
        if (bin < 0)   bin = 0;
        if (bin > 255) bin = 255;
        hist[bin]++;
    }

    double H = 0.0;
    double inv_n = 1.0 / (double)n;
    for (int b = 0; b < 256; b++) {
        if (hist[b] == 0) continue;
        double p = (double)hist[b] * inv_n;
        H -= p * log2(p);
    }

    *out_H = H;
    return STRIQ_OK;
}

striq_status_t stats_curvature(
    const double *values,
    size_t        n,
    double       *out_cv)
{
    if (!values || !out_cv) return STRIQ_ERR_PARAM;
    if (n < 3) return STRIQ_ERR_PARAM;

    double mu = 0.0;
    for (size_t i = 0; i < n; i++) mu += values[i];
    mu /= (double)n;

    double var_v = 0.0;
    for (size_t i = 0; i < n; i++) {
        double d = values[i] - mu;
        var_v += d * d;
    }
    var_v /= (double)n;

    if (var_v < 1e-30) {
        /* Constant series — curvature undefined, treat as 0 (fully linear) */
        *out_cv = 0.0;
        return STRIQ_ERR_CODEC;
    }

    /* Compute Var(d2) where d2[i] = values[i+2] - 2*values[i+1] + values[i] */
    size_t m = n - 2;  /* number of second-difference points */
    double mu_d2 = 0.0;
    for (size_t i = 0; i < m; i++)
        mu_d2 += values[i + 2] - 2.0 * values[i + 1] + values[i];
    mu_d2 /= (double)m;

    double var_d2 = 0.0;
    for (size_t i = 0; i < m; i++) {
        double d2 = values[i + 2] - 2.0 * values[i + 1] + values[i] - mu_d2;
        var_d2 += d2 * d2;
    }
    var_d2 /= (double)m;

    *out_cv = var_d2 / var_v;
    return STRIQ_OK;
}

uint32_t stats_unique_count(
    const double *values,
    size_t        n,
    uint32_t      max_unique)
{
    if (!values || n == 0) return 0;

    /* Inline table sized for max_unique up to 64 (RLE_UNIQUE_THRESHOLD). */
    double seen[65];
    uint32_t nu = 0;

    /* Cap at max_unique to avoid overflowing the table. */
    uint32_t cap = (max_unique < 64u) ? max_unique : 64u;

    for (size_t i = 0; i < n; i++) {
        double v = values[i];
        int found = 0;
        for (uint32_t j = 0; j < nu; j++) {
            if (seen[j] == v) { found = 1; break; }
        }
        if (!found) {
            if (nu >= cap) return cap + 1; /* early exit: too many */
            seen[nu++] = v;
        }
    }
    return nu;
}
