#ifndef __EBLOB_CRC32_H
#define __EBLOB_CRC32_H

#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

uint32_t crc32_4bytes(const void* data, size_t length, uint32_t previousCrc32 = 0);
uint32_t crc32_8bytes(const void* data, size_t length, uint32_t previousCrc32 = 0);
uint32_t crc32_16bytes(const void* data, size_t length, uint32_t previousCrc32 = 0);
uint32_t crc32_fast(const void* data, size_t length, uint32_t previousCrc32 = 0);


#ifdef __cplusplus
}
#endif

#endif /* __EBLOB_CRC32_H */