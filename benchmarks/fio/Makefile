
THIS_MAKEFILE_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
ROOT_DIR := $(abspath $(THIS_MAKEFILE_DIR)/../..)

PHOENIX_INCLUDE_DIR := ${ROOT_DIR}/libphoenix/include
PHOENIX_LIB_DIR := ${ROOT_DIR}/build
FIO_SRC_DIR ?= ${ROOT_DIR}/third-party/fio
CUDA_HOME ?= /usr/local/cuda
CUDA_LIB_DIR := ${CUDA_HOME}/lib64
CUDA_INC_DIR := ${CUDA_HOME}/include

PLUGIN_NAME = phoenix_fio
SO_NAME = ${PLUGIN_NAME}.so
SRC = ${PLUGIN_NAME}.cpp
OBJ = ${PLUGIN_NAME}.o

CXX = g++
CXXFLAGS = -fPIC -fpermissive -O3 -D_GNU_SOURCE -shared -rdynamic -I${PHOENIX_INCLUDE_DIR} -I${FIO_SRC_DIR} -I${CUDA_INC_DIR} -include ${ROOT_DIR}/third-party/fio/config-host.h
LDFLAGS = -L${PHOENIX_LIB_DIR} -lphoenix -Wl,-rpath=${PHOENIX_LIB_DIR} -L${CUDA_LIB_DIR} -lcudart -luring

.PHONY: all clean

all: ${SO_NAME}

${SO_NAME}: ${SRC}
	@echo "Building shared object: ${ROOT_DIR}"
	${CXX} ${CXXFLAGS} $^ -o $@ ${LDFLAGS}

clean:
	rm -rf ${OBJ} ${SO_NAME}
