#!/usr/bin/env python

import torch
import os
import sys
import time
import ctypes

from phxfs import PhxfsDriver, Phxfs
from lmcache.v1.memory_management import GPUMemoryAllocator

DEVICE_ID = 0 # only support the device id 0 for now.
DATA_PATH = '/mnt/phxfs/data.bin'

class PhxfsMemoryAllocator(GPUMemoryAllocator):
    def __init__(self, size: int, device=None):
        from phxfs.phxfs_bind import phxfs_regmem, phxfs_deregmem

        self.phxfsBufDeReg = phxfs_deregmem
        if device is None:
            if torch.cuda.is_available():
                device = f"cuda:{torch.cuda.current_device()}"
            else:
                device = "cpu:0"
        super().__init__(size, device, align_bytes=4096)
        self.size = size
        self.device = DEVICE_ID 
        self.base_pointer = self.tensor.data_ptr()
        void_ptr = ctypes.c_void_p()
        host_ptr = ctypes.POINTER(ctypes.c_void_p)(void_ptr)
        phxfs_regmem(self.device, ctypes.c_void_p(self.base_pointer), ctypes.c_size_t(self.size), host_ptr)
        self.host_ptr = void_ptr

    def __del__(self):
        self.phxfsBufDeReg(self.device, ctypes.c_void_p(self.base_pointer), ctypes.c_size_t(self.size))

    def __str__(self):
        return "PhxfsMemoryAllocator"


if __name__ == "__main__":


    phxfs_driver = PhxfsDriver(DEVICE_ID)

    shape = torch.Size([2, 36, 256, 1024])
    dtype = torch.bfloat16 
    alloc = PhxfsMemoryAllocator(37748736, device="cuda:%d" % DEVICE_ID)

    
    with Phxfs(DATA_PATH, "r", use_direct_io=True, device_id=DEVICE_ID) as f:
        start = time.time()
        r = f.read(
            alloc.base_pointer,
            37748736,
            file_offset=0,
            dev_offset=0,
        )
        load_du = time.time() - start
        print(f"read data take {load_du:.6f}s")

    alloc = None
    phxfs_driver = None
