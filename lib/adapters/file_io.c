#include "file_io.h"
#include <stdio.h>

striq_status_t file_io_write(const uint8_t *data, size_t len, void *ctx)
{
    striq_file_ctx_t *fc = (striq_file_ctx_t *)ctx;
    if (fwrite(data, 1, len, fc->fp) != len) return STRIQ_ERR_IO;
    return STRIQ_OK;
}

striq_status_t file_io_read(uint8_t *buf, size_t len, void *ctx)
{
    striq_file_ctx_t *fc = (striq_file_ctx_t *)ctx;
    if (fread(buf, 1, len, fc->fp) != len) return STRIQ_ERR_IO;
    return STRIQ_OK;
}

striq_status_t file_io_seek(int64_t offset, int whence, void *ctx)
{
    striq_file_ctx_t *fc = (striq_file_ctx_t *)ctx;
    if (fseek(fc->fp, (long)offset, whence) != 0) return STRIQ_ERR_IO;
    return STRIQ_OK;
}

uint64_t file_io_size(FILE *fp)
{
    long cur = ftell(fp);
    if (cur < 0) return 0;
    if (fseek(fp, 0, SEEK_END) != 0) return 0;
    long sz = ftell(fp);
    fseek(fp, cur, SEEK_SET);
    return (sz < 0) ? 0 : (uint64_t)sz;
}
