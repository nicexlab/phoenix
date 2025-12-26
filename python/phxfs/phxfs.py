"""
Main module for Phxfs file operations.
"""

import os
import ctypes

from .phxfs_bind import *


def _singleton(cls):
    _instances = {}

    def wrapper(*args, **kwargs):
        if cls not in _instances:
            _instances[cls] = cls(*args, **kwargs)
        return _instances[cls]

    return wrapper


@_singleton
class PhxfsDriver:
    def __init__(self, device_id: int):
        self.device_id = device_id
        r = phxfs_open(device_id)
        if r != 0:
            raise RuntimeError("open phxfs error %d".format(r))

    def __del__(self):
        r = phxfs_close(self.device_id)
        if r != 0:
            raise RuntimeError("open phxfs error %d".format(r))


def _os_mode(mode: str):
    modes = {
        "r": os.O_RDONLY,
        "r+": os.O_RDWR,
        "w": os.O_CREAT | os.O_WRONLY | os.O_TRUNC,
        "w+": os.O_CREAT | os.O_RDWR | os.O_TRUNC,
        "a": os.O_CREAT | os.O_WRONLY | os.O_APPEND,
        "a+": os.O_CREAT | os.O_RDWR | os.O_APPEND,
    }
    return modes[mode]


class Phxfs:
    """
    Main class for Phxfs file operations.
    """

    def __init__(self, path: str, mode: str = "r", use_direct_io: bool = False, device_id = 0):
        """
        Initialize the Phxfs instance.
        """
        self._device_id = device_id
        self._path = path
        self._mode = mode
        self._os_mode = _os_mode(mode)
        if use_direct_io:
            self._os_mode |= os.O_DIRECT

        self._handle = None

    def __del__(self):
        """
        Destructor to ensure the file is closed.
        """
        if self.is_open:
            self.close()

    @property
    def is_open(self) -> bool:
        return self._handle is not None

    def open(self):
        """Opens the file and registers the handle."""
        if self.is_open:
            return
        self._handle = os.open(self._path, self._os_mode)
        self.phxfs_fileid = phxfs_fileid_t(fd=self._handle, deviceID=self._device_id)

    def close(self):
        """Deregisters the handle and closes the file."""
        if not self.is_open:
            return

        os.close(self._handle)
        self._handle = None

    def regmem(self, addr, nbytes, target_addr):
        return phxfs_regmem(self._device_id, addr, nbytes, target_addr)

    def deregmem(self, addr, nbytes):
        return phxfs_deregmem(self._device_id, addr, nbytes)

    def __enter__(self):
        """Context manager entry."""
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def read(self, dest: ctypes.c_void_p, size: int, file_offset: int = 0, dev_offset: int = 0):
        """Read from the file."""
        if not self.is_open:
            raise IOError("File is not open.")
        return phxfs_read(self.phxfs_fileid, dest, dev_offset, size, file_offset)

    def write(self, src: ctypes.c_void_p, size: int, file_offset: int = 0, dev_offset: int = 0):
        """Write to the file."""
        if not self.is_open:
            raise IOError("File is not open.")
        return phxfs_write(self.phxfs_fileid, src, dev_offset, size, file_offset)

    def get_handle(self):
        """Get the file handle."""
        return self._handle
