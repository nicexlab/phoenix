#ifndef __PHOENIX_H__
#define __PHOENIX_H__
#include <cstddef>
#include <cstdint>
#include <stdint.h>
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <cuda.h>
#include <cuda_runtime.h>
#include "common.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct phxfs_fileid {
    int fd;
    int deviceID;
} phxfs_fileid_t;

struct xfer_addr {
    void *target_addr;
    size_t nbyte;
};

#define MAX_NR_ADDR 4
struct phxfs_xfer_addr{
    uint32_t nr_xfer_addrs;
    struct xfer_addr x_addrs[1];
};

// para for a single io operation

int phxfs_open(int device_id);
int phxfs_close(int device_id);
ssize_t phxfs_read(phxfs_fileid_t fid, void *buf, off_t buf_offset, ssize_t nbyte, off_t f_offset);
ssize_t phxfs_write(phxfs_fileid_t fid, void *buf, off_t buf_offset, ssize_t nbyte, off_t f_offset);

struct phxfs_xfer_addr * phxfs_do_xfer_addr(int device_id, const void *buf, off_t buf_offset, size_t nbyte);
int phxfs_regmem(int device_id, const void *addr, size_t len, void **target_addr);
int phxfs_deregmem(int device, const void *addr, size_t len);

cudaError_t phxfs_read_async(phxfs_fileid_t fid,
                            void* buf,
                            size_t nbytes, off_t offset,
                            ssize_t *bytes_done,
                            CUstream stream);

cudaError_t phxfs_write_async(phxfs_fileid_t fid,
                            void* buf,
                            size_t nbytes, off_t offset,
                            ssize_t *bytes_done,
                            CUstream stream);

#ifdef __cplusplus
}
#endif

#endif
