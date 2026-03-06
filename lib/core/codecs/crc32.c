#include "crc32.h"
#include "../../platform/simd.h"

/*
 * striq_crc32c(crc, data, len) inverts both its input and output:
 *   result = ~table_update(~crc, data)
 *
 * The original crc32c_update(crc, data, len) returned table_update(crc, data).
 * To recover that from striq_crc32c we invert both sides:
 *   table_update(crc, data) = ~striq_crc32c(~crc, data)
 */
uint32_t crc32c_update(uint32_t crc, const uint8_t *data, size_t len)
{
    return striq_crc32c(crc ^ 0xFFFFFFFFu, data, len) ^ 0xFFFFFFFFu;
}

/* Standard CRC32C: seed = 0xFFFFFFFF, finalize with XOR 0xFFFFFFFF. */
uint32_t crc32c(const uint8_t *data, size_t len)
{
    return striq_crc32c(0u, data, len);
}
