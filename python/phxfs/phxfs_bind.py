import ctypes
import os

ctypes.CDLL("libcudart.so", mode=ctypes.RTLD_GLOBAL)
ctypes.CDLL("libcuda.so", mode=ctypes.RTLD_GLOBAL)
ctypes.CDLL("libphoenix.so", mode=ctypes.RTLD_GLOBAL)

libphxfs = ctypes.CDLL("libphoenix.so")
cuda = ctypes.CDLL("libcuda.so")

class phxfs_fileid_t(ctypes.Structure):
    _fields_ = [("fd", ctypes.c_int), ("deviceID", ctypes.c_int)]
class xfer_addr(ctypes.Structure):
    _fields_ = [("target_addr", ctypes.c_void_p), ("nbyte", ctypes.c_size_t)]

MAX_NR_ADDR = 4
class phxfs_xfer_addr(ctypes.Structure):
    _fields_ = [("nr_xfer_addrs", ctypes.c_uint32), ("x_addrs", xfer_addr*1)]

cudaError_t = ctypes.c_int

libphxfs.phxfs_open.restype                   = ctypes.c_int
libphxfs.phxfs_close.restype                  = ctypes.c_int
libphxfs.phxfs_read.restype                   = ctypes.c_ssize_t
libphxfs.phxfs_write.restype                  = ctypes.c_ssize_t
libphxfs.phxfs_do_xfer_addr.restype           = ctypes.POINTER(phxfs_xfer_addr)
libphxfs.phxfs_regmem.restype                 = ctypes.c_int
libphxfs.phxfs_deregmem.restype               = ctypes.c_int
libphxfs.phxfs_read_async.restype             = cudaError_t
libphxfs.phxfs_write_async.restype            = cudaError_t

CUstream = ctypes.c_void_p

libphxfs.phxfs_open.argtypes = [ctypes.c_int]
libphxfs.phxfs_close.argtypes = [ctypes.c_int]
libphxfs.phxfs_read.argtypes = [phxfs_fileid_t, ctypes.c_void_p, ctypes.c_longlong, ctypes.c_size_t, ctypes.c_longlong]
libphxfs.phxfs_write.argtypes = [phxfs_fileid_t, ctypes.c_void_p, ctypes.c_longlong, ctypes.c_size_t, ctypes.c_longlong]
libphxfs.phxfs_do_xfer_addr.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_longlong, ctypes.c_size_t]
libphxfs.phxfs_regmem.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_size_t, ctypes.POINTER(ctypes.c_void_p)]
libphxfs.phxfs_deregmem.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_size_t]
libphxfs.phxfs_read_async.argtypes = [phxfs_fileid_t, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_longlong, ctypes.POINTER(ctypes.c_ssize_t), CUstream]
libphxfs.phxfs_write_async.argtypes = [phxfs_fileid_t, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_longlong, ctypes.POINTER(ctypes.c_ssize_t), CUstream]

def _check_ret(ret, name):
    if ret < 0:
        raise RuntimeError(f"{name} failed with return code: {ret}")
def phxfs_open(device_id: int) -> ctypes.c_int:
    ret = libphxfs.phxfs_open(device_id)
    _check_ret(ret, "phxfs_open")
    return ret

def phxfs_close(device_id: int) -> ctypes.c_int:
    ret = libphxfs.phxfs_close(device_id)
    _check_ret(ret, "phxfs_close")
    return ret

def phxfs_read(fid: phxfs_fileid_t, buf: ctypes.c_void_p, buf_offset: ctypes.c_longlong, nbyte: ctypes.c_ssize_t, f_offset: ctypes.c_longlong) -> ctypes.c_ssize_t:
    ret = libphxfs.phxfs_read(fid, buf,  buf_offset, nbyte, f_offset)
    if ret < 0:
        raise RuntimeError(f"phxfs_read failed with return code: {ret}")
    return ret

def phxfs_write(fid: phxfs_fileid_t, buf: ctypes.c_void_p, buf_offset: ctypes.c_longlong, nbyte: ctypes.c_ssize_t, f_offset: ctypes.c_longlong) -> ctypes.c_ssize_t:
    ret = libphxfs.phxfs_write(fid, buf, buf_offset, nbyte, f_offset)
    if ret < 0:
        raise RuntimeError(f"phxfs_write failed with return code: {ret}")
    return ret

def phxfs_do_xfer_addr(device_id: ctypes.c_int, buf: ctypes.c_void_p, buf_offset: ctypes.c_longlong, nbyte: ctypes.c_size_t) -> ctypes.POINTER(phxfs_xfer_addr):
    return libphxfs.phxfs_do_xfer_addr(device_id, buf, buf_offset, nbyte)

def phxfs_regmem(device_id: ctypes.c_int, gpu_buffer: ctypes.c_void_p, len: ctypes.c_size_t, target_addr: ctypes.POINTER(ctypes.c_void_p)) -> ctypes.c_int:
    ret = libphxfs.phxfs_regmem(device_id, gpu_buffer, len, target_addr)
    if ret < 0:
        raise RuntimeError(f"phxfs_regmem failed with return code: {ret}")
    return ret

def phxfs_deregmem(device_id: ctypes.c_int, addr: ctypes.c_void_p, len: ctypes.c_size_t) -> ctypes.c_int:
    ret = libphxfs.phxfs_deregmem(device_id, addr, len)
    if ret < 0:
        raise RuntimeError(f"phxfs_deregmem failed with return code: {ret}")
    return ret

def phxfs_read_async(fid: phxfs_fileid_t, buf: ctypes.c_void_p, nbytes: ctypes.c_size_t, offset: ctypes.c_longlong, bytes_done: ctypes.c_ssize_t, stream: CUstream) -> cudaError_t:
    return libphxfs.phxfs_read_async(fid, buf, nbytes, offset, bytes_done, stream)

def phxfs_write_async(fid: phxfs_fileid_t, buf: ctypes.c_void_p, nbytes: ctypes.c_size_t, offset: ctypes.c_longlong, bytes_done: ctypes.c_ssize_t, stream: CUstream) -> cudaError_t:
    return libphxfs.phxfs_write_async(fid, buf, nbytes, offset, bytes_done, stream)
