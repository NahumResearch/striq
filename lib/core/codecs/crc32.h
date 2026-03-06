#ifndef STRIQ_CRC32_H
#define STRIQ_CRC32_H

/*
 * Thin wrappers over striq_crc32c() (simd.h).
 * Hardware CRC32C (ARM CRC intrinsic) is used when available; falls
 * back to a software Castagnoli table otherwise.
 */

#include <stdint.h>
#include <stddef.h>

uint32_t crc32c(const uint8_t *data, size_t len);
uint32_t crc32c_update(uint32_t crc, const uint8_t *data, size_t len);

#endif /* STRIQ_CRC32_H */
