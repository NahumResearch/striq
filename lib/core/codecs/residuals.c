#include "residuals.h"
#include "zigzag.h"
#include "varint.h"
#include <string.h>
#include <stdlib.h>

#define zz_enc  zigzag_encode
#define zz_dec  zigzag_decode
#define vint_write varint_write_u64
#define vint_read  varint_read_u64

striq_status_t residuals_encode_rle(
    const int64_t *residuals, size_t n,
    uint8_t *out, size_t cap, size_t *out_len)
{
    size_t pos       = 0;
    size_t zero_run  = 0;

    for (size_t i = 0; i <= n; i++) {
        uint64_t zz = (i < n) ? zz_enc(residuals[i]) : 1u;

        if (zz == 0u) {
            zero_run++;
        } else {
            if (zero_run > 0) {
                if (pos >= cap) return STRIQ_ERR_MEMORY;
                out[pos++] = 0x00u;
                size_t w = vint_write((uint64_t)zero_run, out + pos, cap - pos);
                if (w == 0) return STRIQ_ERR_MEMORY;
                pos += w;
                zero_run = 0;
            }
            if (i < n) {
                size_t w = vint_write(zz, out + pos, cap - pos);
                if (w == 0) return STRIQ_ERR_MEMORY;
                pos += w;
            }
        }
    }

    *out_len = pos;
    return STRIQ_OK;
}

static striq_status_t encode_rle_i8(
    const int64_t *residuals, size_t n,
    uint8_t *out, size_t cap, size_t *out_len)
{
    size_t pos      = 0;
    size_t zero_run = 0;

    for (size_t i = 0; i <= n; i++) {
        int64_t r = (i < n) ? residuals[i] : 1;

        if (r == 0) {
            zero_run++;
        } else {
            if (zero_run > 0) {
                if (pos >= cap) return STRIQ_ERR_MEMORY;
                out[pos++] = 0x00u;
                size_t w = vint_write((uint64_t)zero_run, out + pos, cap - pos);
                if (w == 0) return STRIQ_ERR_MEMORY;
                pos += w;
                zero_run = 0;
            }
            if (i < n) {
                if (pos >= cap) return STRIQ_ERR_MEMORY;
                out[pos++] = (uint8_t)(int8_t)r;
            }
        }
    }

    *out_len = pos;
    return STRIQ_OK;
}

static striq_status_t encode_rle_i16(
    const int64_t *residuals, size_t n,
    uint8_t *out, size_t cap, size_t *out_len)
{
    size_t pos      = 0;
    size_t zero_run = 0;

    for (size_t i = 0; i <= n; i++) {
        int64_t r = (i < n) ? residuals[i] : 1;

        if (r == 0) {
            zero_run++;
        } else {
            if (zero_run > 0) {
                /* Use a 2-byte marker: 0x00 0x00 = zero run */
                if (pos + 1 >= cap) return STRIQ_ERR_MEMORY;
                out[pos++] = 0x00u; out[pos++] = 0x00u;
                size_t w = vint_write((uint64_t)zero_run, out + pos, cap - pos);
                if (w == 0) return STRIQ_ERR_MEMORY;
                pos += w;
                zero_run = 0;
            }
            if (i < n) {
                if (pos + 1 >= cap) return STRIQ_ERR_MEMORY;
                int16_t v16 = (int16_t)r;
                out[pos++] = (uint8_t)(v16 & 0xFF);
                out[pos++] = (uint8_t)((v16 >> 8) & 0xFF);
            }
        }
    }

    *out_len = pos;
    return STRIQ_OK;
}

striq_status_t residuals_encode_auto(
    const int64_t *residuals, size_t n,
    uint8_t *out, size_t cap, size_t *out_len)
{
    if (n == 0) { *out_len = 0; return STRIQ_OK; }
    if (!out || cap < 2u) return STRIQ_ERR_MEMORY;

    int64_t max_abs = 0;
    for (size_t i = 0; i < n; i++) {
        int64_t a = residuals[i] < 0 ? -residuals[i] : residuals[i];
        if (a > max_abs) max_abs = a;
    }

    int width;
    uint8_t flag_rle;

    if (max_abs == 0) {
        width = 8; flag_rle = RESID_FLAG_RLE_I8;
    } else if (max_abs <= 127) {
        width = 8; flag_rle = RESID_FLAG_RLE_I8;
    } else if (max_abs <= 32767) {
        width = 16; flag_rle = RESID_FLAG_RLE_I16;
    } else {
        width = 32; flag_rle = RESID_FLAG_RLE;
    }

    size_t raw_cap = n * 10u + 16u;
    uint8_t *raw_tmp = malloc(raw_cap);
    if (!raw_tmp) return STRIQ_ERR_MEMORY;

    size_t raw_len = 0;
    striq_status_t s;

    if (width == 8) {
        s = encode_rle_i8(residuals, n, raw_tmp, raw_cap, &raw_len);
    } else if (width == 16) {
        s = encode_rle_i16(residuals, n, raw_tmp, raw_cap, &raw_len);
    } else {
        s = residuals_encode_rle(residuals, n, raw_tmp, raw_cap, &raw_len);
    }

    if (s != STRIQ_OK) { free(raw_tmp); return s; }

    striq_status_t ret;
    if (1u + raw_len > cap) {
        ret = STRIQ_ERR_MEMORY;
    } else {
        out[0] = flag_rle;
        memcpy(out + 1, raw_tmp, raw_len);
        *out_len = 1u + raw_len;
        ret = STRIQ_OK;
    }

    free(raw_tmp);
    return ret;
}

static striq_status_t decode_rle_i8(
    const uint8_t *in, size_t in_len,
    int64_t *out, size_t n)
{
    size_t rpos = 0, opos = 0;
    while (rpos < in_len && opos < n) {
        if (in[rpos] == 0x00u) {
            rpos++;
            uint64_t count = 0;
            size_t c = vint_read(in + rpos, in_len - rpos, &count);
            if (c == 0) return STRIQ_ERR_FORMAT;
            rpos += c;
            for (uint64_t k = 0; k < count && opos < n; k++)
                out[opos++] = 0;
        } else {
            out[opos++] = (int64_t)(int8_t)in[rpos++];
        }
    }
    return (opos == n) ? STRIQ_OK : STRIQ_ERR_FORMAT;
}

static striq_status_t decode_rle_i16(
    const uint8_t *in, size_t in_len,
    int64_t *out, size_t n)
{
    size_t rpos = 0, opos = 0;
    while (rpos + 1 < in_len && opos < n) {
        if (in[rpos] == 0x00u && in[rpos+1] == 0x00u) {
            rpos += 2;
            uint64_t count = 0;
            size_t c = vint_read(in + rpos, in_len - rpos, &count);
            if (c == 0) return STRIQ_ERR_FORMAT;
            rpos += c;
            for (uint64_t k = 0; k < count && opos < n; k++)
                out[opos++] = 0;
        } else {
            int16_t v16 = (int16_t)(in[rpos] | ((uint16_t)in[rpos+1] << 8));
            out[opos++] = (int64_t)v16;
            rpos += 2;
        }
    }
    /* If only one byte left and it's non-zero this is an error */
    return (opos == n) ? STRIQ_OK : STRIQ_ERR_FORMAT;
}

static striq_status_t decode_rle_varint(
    const uint8_t *in, size_t in_len,
    int64_t *out, size_t n)
{
    size_t rpos = 0, opos = 0;
    while (rpos < in_len && opos < n) {
        if (in[rpos] == 0x00u) {
            rpos++;
            uint64_t count = 0;
            size_t c = vint_read(in + rpos, in_len - rpos, &count);
            if (c == 0) return STRIQ_ERR_FORMAT;
            rpos += c;
            for (uint64_t k = 0; k < count && opos < n; k++)
                out[opos++] = 0;
        } else {
            uint64_t zz = 0;
            size_t c = vint_read(in + rpos, in_len - rpos, &zz);
            if (c == 0) return STRIQ_ERR_FORMAT;
            rpos += c;
            out[opos++] = zz_dec(zz);
        }
    }
    return (opos == n) ? STRIQ_OK : STRIQ_ERR_FORMAT;
}

striq_status_t residuals_decode(
    const uint8_t *in, size_t in_len,
    int64_t *out, size_t n)
{
    if (n == 0) return STRIQ_OK;
    if (!in || in_len == 0) return STRIQ_ERR_FORMAT;

    uint8_t flag     = in[0];
    const uint8_t *payload = in + 1;
    size_t         pay_len = in_len - 1;

    switch (flag) {
        case RESID_FLAG_RLE:
            return decode_rle_varint(payload, pay_len, out, n);
        case RESID_FLAG_RLE_I8:
            return decode_rle_i8(payload, pay_len, out, n);
        case RESID_FLAG_RLE_I16:
            return decode_rle_i16(payload, pay_len, out, n);
        default:
            return STRIQ_ERR_FORMAT;
    }
}
