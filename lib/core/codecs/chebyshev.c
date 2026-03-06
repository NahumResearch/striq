#include "chebyshev.h"
#include <math.h>
#include <string.h>


double cheb3_eval(const double c[4], double u)
{
    /*
     * Clenshaw recurrence for sum c[k]*T_k(u), k=0..3.
     * b_{n+1} = 0, b_n = c_n
     * b_{k} = c_k + 2*u*b_{k+1} - b_{k+2}   for k = n-1 down to 1
     * result = c_0 + u*b_1 - b_2
     */
    double b1 = c[3];
    double b2 = 2.0*u*b1 + c[2];
    double tmp = b2;
    b2 = 2.0*u*b2 - b1 + c[1];
    b1 = tmp;
    return u*b2 - b1 + c[0];
}

void cheb3_eval_range(const double c[4], uint16_t length, double *out_values)
{
    if (length == 0) return;
    if (length == 1) { out_values[0] = cheb3_eval(c, 0.0); return; }

    double inv = 2.0 / (double)(length - 1);
    for (uint16_t j = 0; j < length; j++) {
        double u = inv * (double)j - 1.0;
        out_values[j] = cheb3_eval(c, u);
    }
}

static int solve_4x4(double A[4][4], double b[4], double x[4])
{
    double M[4][5];
    int    i, j, r, pvt;

    for (i = 0; i < 4; i++) {
        for (j = 0; j < 4; j++) M[i][j] = A[i][j];
        M[i][4] = b[i];
    }

    for (int col = 0; col < 4; col++) {
        pvt = col;
        for (r = col + 1; r < 4; r++)
            if (fabs(M[r][col]) > fabs(M[pvt][col])) pvt = r;
        if (fabs(M[pvt][col]) < 1e-15) return -1;

        for (j = 0; j <= 4; j++) {
            double t = M[col][j]; M[col][j] = M[pvt][j]; M[pvt][j] = t;
        }

        for (r = 0; r < 4; r++) {
            if (r == col) continue;
            double f = M[r][col] / M[col][col];
            for (j = col; j <= 4; j++) M[r][j] -= f * M[col][j];
        }
    }

    for (i = 0; i < 4; i++) x[i] = M[i][4] / M[i][i];
    return 0;
}

static void cheb3_compute_coeffs(const double *values, size_t L, double c[4])
{
    /*
     * Compute Chebyshev coefficients by solving the 4×4 normal equations:
     *   G c = b
     * where G[m][k] = Σ T_m(u_j) T_k(u_j)  and  b[m] = Σ f_j T_m(u_j).
     *
     * Using the Gram matrix (rather than the simple projection formula)
     * makes this correct for any grid, not just Chebyshev nodes.
     */
    double G[4][4] = {{0,0,0,0},{0,0,0,0},{0,0,0,0},{0,0,0,0}};
    double b[4] = {0,0,0,0};
    int m, k;

    if (L == 0) { c[0]=c[1]=c[2]=c[3]=0.0; return; }
    if (L == 1) { c[0]=values[0]; c[1]=c[2]=c[3]=0.0; return; }

    double inv = 2.0 / (double)(L - 1);
    for (size_t j = 0; j < L; j++) {
        double u  = inv * (double)j - 1.0;
        double u2 = u * u;
        double T[4] = { 1.0, u, 2.0*u2 - 1.0, u*(4.0*u2 - 3.0) };
        for (m = 0; m < 4; m++) {
            b[m] += values[j] * T[m];
            for (k = m; k < 4; k++) {
                double g = T[m] * T[k];
                G[m][k] += g;
                if (k != m) G[k][m] += g;
            }
        }
    }

    if (solve_4x4(G, b, c) != 0) {

        double s = 0.0;
        for (size_t j = 0; j < L; j++) s += values[j];
        c[0] = s / (double)L;
        c[1] = c[2] = c[3] = 0.0;
    }
}

striq_status_t cheb3_fit(
    const double *values,
    size_t        count,
    double        epsilon_b,
    double        c[4])
{
    if (!values || count == 0 || !c) return STRIQ_ERR_PARAM;

    /* Need at least 4 points to fit a cubic meaningfully */
    if (count < 4) {
        cheb3_compute_coeffs(values, count, c);
    } else {
        cheb3_compute_coeffs(values, count, c);
    }

    if (count == 1) return STRIQ_OK;

    double inv = 2.0 / (double)(count - 1);
    for (size_t j = 0; j < count; j++) {
        double u     = inv * (double)j - 1.0;
        double approx = cheb3_eval(c, u);
        double err   = values[j] - approx;
        if (err < 0.0) err = -err;
        if (err > epsilon_b) return STRIQ_ERR_CODEC;
    }

    return STRIQ_OK;
}

size_t cheb3_find_max_length(
    const double *values,
    size_t        start,
    size_t        min_len,
    size_t        max_len,
    double        epsilon_b,
    double        c[4])
{
    double tmp_c[4];
    if (max_len < min_len) return 0;
    if (min_len < 4) min_len = 4;  /* cubic needs at least 4 points */
    if (max_len < min_len) return 0;

    if (cheb3_fit(values + start, min_len, epsilon_b, tmp_c) != STRIQ_OK)
        return 0;

    size_t lo = min_len;
    size_t hi = min_len;
    while (hi < max_len) {
        size_t next = (hi * 2 < max_len) ? hi * 2 : max_len;
        if (cheb3_fit(values + start, next, epsilon_b, tmp_c) == STRIQ_OK) {
            lo = next;
            hi = next;
        } else {
            break;
        }
    }

    if (hi == max_len &&
        cheb3_fit(values + start, max_len, epsilon_b, tmp_c) == STRIQ_OK) {
        if (c) { c[0]=tmp_c[0]; c[1]=tmp_c[1]; c[2]=tmp_c[2]; c[3]=tmp_c[3]; }
        return max_len;
    }

    while (lo + 1 < hi) {
        size_t mid = (lo + hi) / 2;
        if (cheb3_fit(values + start, mid, epsilon_b, tmp_c) == STRIQ_OK)
            lo = mid;
        else
            hi = mid;
    }

    if (cheb3_fit(values + start, lo, epsilon_b, tmp_c) == STRIQ_OK) {
        if (c) { c[0]=tmp_c[0]; c[1]=tmp_c[1]; c[2]=tmp_c[2]; c[3]=tmp_c[3]; }
        return lo;
    }
    return 0;
}
