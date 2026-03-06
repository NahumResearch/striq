#ifndef STRIQ_GORILLA_H
#define STRIQ_GORILLA_H

/*
 * Gorilla XOR compression — VLDB 2015 reference implementation.
 * Bench-only: not part of the core codec pipeline.
 *
 * Encodes a sequence of double-precision floats using XOR delta coding with
 * leading/trailing zero optimization as described in the Gorilla paper.
 */

#include <stddef.h>
#include <stdint.h>

/*
 * Compress N doubles into out[0..cap-1].
 * Returns the number of bytes written, or 0 if cap is too small.
 */
size_t gorilla_compress(const double *values, size_t count,
                        uint8_t *out, size_t cap);

/*
 * Decompress data into out[0..out_cap-1].
 * Returns the number of doubles decoded, or 0 on error.
 * The caller must pass the original count (stored externally, e.g. in a header).
 */
size_t gorilla_decompress(const uint8_t *data, size_t data_size,
                          size_t expected_count,
                          double *out, size_t out_cap);

#endif /* STRIQ_GORILLA_H */
