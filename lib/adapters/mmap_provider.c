#include "mmap_provider.h"

#ifdef _WIN32


striq_status_t mmap_provider_open(
    striq_mmap_provider_t *p,
    striq_fmt_reader_t    *fmt,
    const char            *path)
{
    (void)p; (void)fmt; (void)path;
    return STRIQ_ERR_NOTIMPL;
}

void mmap_provider_close(striq_mmap_provider_t *p)
{
    (void)p;
}

#else /* POSIX */

#include "file_io.h"

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>

typedef struct {
    const uint8_t *base;
    size_t         len;
    size_t         pos;
} mmap_io_ctx_t;

static striq_status_t mmap_io_read(uint8_t *buf, size_t n, void *ctx)
{
    mmap_io_ctx_t *m = (mmap_io_ctx_t *)ctx;
    if (m->pos + n > m->len) return STRIQ_ERR_IO;
    memcpy(buf, m->base + m->pos, n);
    m->pos += n;
    return STRIQ_OK;
}

static striq_status_t mmap_io_seek(int64_t offset, int whence, void *ctx)
{
    mmap_io_ctx_t *m = (mmap_io_ctx_t *)ctx;
    int64_t new_pos;
    if (whence == 0 /* SEEK_SET */) {
        new_pos = offset;
    } else if (whence == 1 /* SEEK_CUR */) {
        new_pos = (int64_t)m->pos + offset;
    } else {
        new_pos = (int64_t)m->len + offset;
    }
    if (new_pos < 0 || (size_t)new_pos > m->len) return STRIQ_ERR_IO;
    m->pos = (size_t)new_pos;
    return STRIQ_OK;
}

static striq_status_t mp_get_block_index(
    void *ctx, uint32_t idx, striq_block_index_t *out)
{
    striq_mmap_provider_t *mp  = (striq_mmap_provider_t *)ctx;
    striq_fmt_reader_t    *fmt = mp->fmt;
    if (idx >= fmt->num_blocks) return STRIQ_ERR_NOTFOUND;
    *out = fmt->block_index[idx];
    return STRIQ_OK;
}

static striq_status_t mp_get_block_data(
    void *ctx, uint32_t idx, const uint8_t **data, uint32_t *size)
{
    striq_mmap_provider_t *p = (striq_mmap_provider_t *)ctx;
    striq_fmt_reader_t    *fmt = p->fmt;
    if (idx >= fmt->num_blocks) return STRIQ_ERR_NOTFOUND;

    uint64_t off = fmt->block_index[idx].file_offset;
    uint32_t bsz = fmt->block_index[idx].block_size;
    if (off + bsz > p->map_len) return STRIQ_ERR_FORMAT;

    *data = (const uint8_t *)p->map + off;
    *size = bsz;
    return STRIQ_OK;
}

static void mp_release_block(void *ctx, uint32_t idx, const uint8_t *data)
{
    (void)ctx; (void)idx; (void)data;
}

static striq_status_t mp_get_col_stats(
    void *ctx, uint32_t idx, uint32_t col_idx, striq_col_stats_t *out)
{
    striq_mmap_provider_t *mp  = (striq_mmap_provider_t *)ctx;
    striq_fmt_reader_t    *fmt = mp->fmt;
    if (idx >= fmt->num_blocks || col_idx >= fmt->num_cols)
        return STRIQ_ERR_NOTFOUND;
    *out = fmt->block_stats[idx][col_idx];
    return STRIQ_OK;
}

striq_status_t mmap_provider_open(
    striq_mmap_provider_t *p,
    striq_fmt_reader_t    *fmt,
    const char            *path)
{
    if (!p || !fmt || !path) return STRIQ_ERR_PARAM;
    memset(p, 0, sizeof(*p));
    p->fd  = -1;
    p->fmt = fmt;

    p->fd = open(path, O_RDONLY);
    if (p->fd < 0) return STRIQ_ERR_IO;

    struct stat st;
    if (fstat(p->fd, &st) != 0) { close(p->fd); p->fd = -1; return STRIQ_ERR_IO; }
    p->map_len = (size_t)st.st_size;

    p->map = mmap(NULL, p->map_len, PROT_READ, MAP_PRIVATE, p->fd, 0);
    if (p->map == MAP_FAILED) {
        p->map = NULL;
        close(p->fd); p->fd = -1;
        return STRIQ_ERR_IO;
    }

    mmap_io_ctx_t io_ctx = {
        .base = (const uint8_t *)p->map,
        .len  = p->map_len,
        .pos  = 0
    };
    striq_status_t s = fmt_reader_open(
        fmt, mmap_io_read, mmap_io_seek, &io_ctx, (uint64_t)p->map_len);
    if (s != STRIQ_OK) {
        munmap(p->map, p->map_len);
        p->map = NULL;
        close(p->fd); p->fd = -1;
        return s;
    }

    p->base.ctx        = p;
    p->base.num_blocks = fmt->num_blocks;
    p->base.num_cols   = fmt->num_cols;
    p->base.epsilon_b  = fmt->epsilon_b;
    p->base.total_rows = fmt->total_rows;
    p->base.col_schemas = fmt->cols;

    for (uint32_t c = 0; c < fmt->num_cols && c < STRIQ_MAX_COLS; c++) {
        double ce = fmt->cols[c].epsilon_b;
        p->base.col_epsilons[c] = (ce > 0.0) ? ce : fmt->epsilon_b;
    }

    p->base.get_block_index = mp_get_block_index;
    p->base.get_block_data  = mp_get_block_data;
    p->base.release_block   = mp_release_block;
    p->base.get_col_stats   = mp_get_col_stats;
    return STRIQ_OK;
}

void mmap_provider_close(striq_mmap_provider_t *p)
{
    if (!p) return;
    if (p->map)   { munmap(p->map, p->map_len); p->map = NULL; }
    if (p->fd >= 0) { close(p->fd); p->fd = -1; }
}

#endif /* _WIN32 */
