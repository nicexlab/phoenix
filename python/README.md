# Python api for phoenix 

This module provide the python api for phoenix.

## Features
- Use ctypes to wrapper the phoenix api from dynamic library of libphoenix.so
- Provide a class for phoenix file operations.

## Installation

```bash
python setup install
```

## Usage

1. compile the libphoenix.so
2. deploy the libphoenix.so to /usr/lib64/ or any other library path
3. python -c "import phxfs;print(phxfs.__file__)"

## License
Apache-2.0 License
