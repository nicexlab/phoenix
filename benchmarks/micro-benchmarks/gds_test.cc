#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <cuda_runtime_api.h>
#include <cufile.h>
#include <builtin_types.h>
#include <pthread.h>
#include <sys/types.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>

#include "cufile_sample_utils.h"
#include "phxfs_utils.h"

static void *write_thread(void *arg) {
    ThreadData *data = (ThreadData *)arg;
    struct timespec io_start, io_end;
    CUfileHandle_t cf_handle = *(CUfileHandle_t*)data->handler;
    ssize_t io_size = (ssize_t)data->io_size;
    size_t written = 0;
    uint64_t io_time;

    pr_info(__func__);
    clock_gettime(CLOCK_MONOTONIC, &data->start_time);
    while (written < data->size) {
        clock_gettime(CLOCK_MONOTONIC, &io_start);
        ssize_t result = cuFileWrite(cf_handle, 
        data->gpu_buffer, data->io_size, 
        data->offset + written,  written);
        
        if (result == 0) {
            // End of file reached
            break;
        }
        if (result != io_size) {
            printf("write_thread error, result is %lu, size is %lu\n",result, data->io_size);
            return NULL;
        }
        // check_cudaruntimecall(cudaStreamSynchronize(0));
        clock_gettime(CLOCK_MONOTONIC, &io_end);
        io_time = (io_end.tv_sec - io_start.tv_sec) * 1000000000LL + (io_end.tv_nsec - io_start.tv_nsec);
        data->latency_vec.push_back(io_time);
        data->total_io_time += io_time;
        data->io_operations++;
        written += result;
    }
    clock_gettime(CLOCK_MONOTONIC, &data->end_time);
    return NULL;
}

static void *read_thread(void *arg) {
    ThreadData *data = (ThreadData *)arg;
    struct timespec io_start, io_end;
    CUfileHandle_t cf_handle = *(CUfileHandle_t*)data->handler;
    ssize_t io_size = (ssize_t)data->io_size;
    size_t read_bytes = 0;
    uint64_t io_time;

    clock_gettime(CLOCK_MONOTONIC, &data->start_time);
    while (read_bytes < data->size) {

        clock_gettime(CLOCK_MONOTONIC, &io_start);
        ssize_t result = cuFileRead(cf_handle,
        data->gpu_buffer, data->io_size,
        data->offset + read_bytes, 0);
        cudaStreamSynchronize(0);
        if (result == 0) {
            // End of file reached
            break;
        }
        if (result != io_size) {
            printf("read_thread error, result is %lu, size is %lu\n",result, data->io_size);
            return NULL;
        }
        clock_gettime(CLOCK_MONOTONIC, &io_end);
        read_bytes += result;
        io_time = (io_end.tv_sec - io_start.tv_sec) * 1000000000LL + (io_end.tv_nsec - io_start.tv_nsec);
        data->latency_vec.push_back(io_time);
        data->io_operations++;
        data->total_io_time += io_time;
    }

    clock_gettime(CLOCK_MONOTONIC, &data->end_time);
    check_cudaruntimecall((cudaFree(data->gpu_buffer)));
    
    return NULL;
}


void *async_thread(void *arg){
    CUfileError_t status;
    struct timespec io_start, io_end;
    ThreadData *data = (ThreadData *)arg;
    io_args_s *io_args = (io_args_s *)malloc( data->depth * sizeof(io_args_s));
    size_t done_bytes = 0;
    ssize_t batch_iter_nbytes = 0;
    cudaStream_t *stream = new cudaStream_t[data->depth];
    CUfileHandle_t cf_handle = *(CUfileHandle_t*)data->handler;

    CUfileError_t (*cuFileRW)(CUfileHandle_t fh, void *bufPtr_base,
                              size_t *size_p, off_t *file_offset_p,
                              off_t *bufPtr_offset_p, ssize_t *bytes_written_p,
                              CUstream stream);
    printf("data %d", data->mode == OP_READ);
    cuFileRW = (data->mode == OP_READ) ? cuFileReadAsync : cuFileWriteAsync;

    for (size_t i = 0; i < data->depth; i++) {
        check_cudaruntimecall(cudaStreamCreateWithFlags(&stream[i], cudaStreamNonBlocking));
        cuFileStreamRegister(stream[i], 15);
    }

    unsigned long long chunk_done_size = 0;
    unsigned long long chunk_size = data->size / data->depth;
    pr_info(__func__);

    int repeated = data->size / data->io_size / data->depth;
    if (repeated < 30){
        repeated = 30 / (repeated) + 1;
        printf("repeated is %d\n", repeated);
    } else {
        repeated = 1;
    }

    while (repeated -- > 0) {
        done_bytes = chunk_done_size = 0;
        while (done_bytes < data->size) {
            if (chunk_done_size + data->io_size > chunk_size) {
                break;
            }
            clock_gettime(CLOCK_MONOTONIC, &io_start);
            for (size_t i = 0; i < data->depth; i++) {
                io_args[i].devPtr = data->gpu_buffers[i];
                io_args[i].io_size = data->io_size;
                io_args[i].f_offset = data->offset + done_bytes + i * data->io_size;
                io_args[i].buf_off =  chunk_done_size;
                io_args[i].bytes_done = 0;
            }
            for (size_t i = 0; i < data->depth; i++) {
                status = cuFileRW(cf_handle, io_args[i].devPtr, &io_args[i].io_size,
                                &io_args[i].f_offset, &io_args[i].buf_off,
                                &io_args[i].bytes_done, stream[0]);
                if (status.err != CU_FILE_SUCCESS) {
                    pr_info("data size: " << data->size << " read_bytes: " << done_bytes << " iter_nbytes: " << batch_iter_nbytes);
                    pr_info("bufPtr: " << io_args[i].devPtr << " io_size: " << io_args[i].io_size << " f_offset: " << io_args[i].f_offset << " buf_off: " << io_args[i].buf_off);
                    std::cerr << "read async failed:"
                            << cuFileGetErrorString(status) << std::endl;
                    return NULL;
                }
            }
        
            check_cudaruntimecall(cudaStreamSynchronize(stream[0]));
            for (size_t i = 0; i < data->depth; i++) {
                done_bytes += io_args[i].bytes_done;
                if (io_args[i].bytes_done != (ssize_t)data->io_size) {
                    pr_error("read_thread error, result is " << io_args[i].bytes_done << ", size is " << data->io_size);
                    return NULL;
                }
            }
            // check_cudaruntimecall(fn)
            clock_gettime(CLOCK_MONOTONIC, &io_end);
            chunk_done_size += data->io_size;
            data->io_operations++;
            unsigned long long io_time = (io_end.tv_sec - io_start.tv_sec) * 1000000000LL + (io_end.tv_nsec - io_start.tv_nsec);
            data->latency_vec.push_back(io_time);
            data->total_io_time += io_time;
            }
    }
    clock_gettime(CLOCK_MONOTONIC, &data->start_time);

    clock_gettime(CLOCK_MONOTONIC, &data->end_time);
    for (size_t i = 0; i < data->depth; i++) {
        check_cudaruntimecall(cudaStreamDestroy(stream[i]));
    }
    return NULL;
}


static void *batch_thread(void *arg){
    struct timespec io_start, io_end;
    CUfileBatchHandle_t batch_handle;
    CUfileError_t status;
    ThreadData *data = (ThreadData *)arg;
    size_t nr_completed = 0;
    size_t done_bytes = 0;
    uint64_t io_time;
    ssize_t chunk_done_size = 0;
    ssize_t chuck_size = data->size / data->depth;

    CUfileIOEvents_t *events = new CUfileIOEvents_t[data->depth];
    CUfileIOParams_t *params = new CUfileIOParams_t[data->depth];

    CUfileHandle_t cf_handle = *(CUfileHandle_t*)data->handler;
    
    CUfileOpcode_t cufile_op = (data->mode == OP_READ) ? CUFILE_READ : CUFILE_WRITE;

    unsigned nr_iter_completed = 0;
    size_t i; 

    memset(&events[0], 0, sizeof(CUfileIOEvents_t));

    // default batch size is 128
    status = cuFileBatchIOSetUp(&batch_handle, data->depth == 1 ? 2 : data->depth);
    if (status.err != CU_FILE_SUCCESS) {
            pr_error("batch setup failed:" << cuFileGetErrorString(status));
            goto deregister_buffer;
    }
     printf("io depth %ld\n", data->depth);
    while (done_bytes < data->size){
        clock_gettime(CLOCK_MONOTONIC, &io_start);
        if ((ssize_t)(chunk_done_size + data->io_size) < chuck_size){
            for(i = 0; i < data->depth; i++) {
                params[i].mode = CUFILE_BATCH;
                params[i].fh = cf_handle;
                params[i].u.batch.devPtr_base = data->gpu_buffers[i];
                params[i].u.batch.devPtr_offset = 0;
                params[i].u.batch.file_offset = data->offset + done_bytes + i * data->io_size;
                params[i].u.batch.size = data->io_size;
                params[i].opcode = cufile_op;
            }
        }else{
            break;
        }
       
        status = cuFileBatchIOSubmit(batch_handle, data->depth, params, 0);
        if (status.err != CU_FILE_SUCCESS) {
            pr_error("batch submit failed:" << cuFileGetErrorString(status));
            goto deregister_buffer;
        }
        nr_completed  = 0;
        nr_iter_completed = data->depth;
        while (nr_completed != data->depth){
            memset(events, 0, sizeof(CUfileIOEvents_t) * data->depth);
            status = cuFileBatchIOGetStatus(batch_handle,(unsigned) data->depth, &nr_iter_completed,
                                          &events[0], NULL);
            if(status.err != 0) {
				std::cerr << "Error in IO Batch Get Status" << std::endl;
                goto batch_destory;
			} 
            nr_completed += nr_iter_completed; 
        }
        clock_gettime(CLOCK_MONOTONIC, &io_end);
        done_bytes += data->depth * data->io_size;
        io_time = (io_end.tv_sec - io_start.tv_sec) * 1000000000LL + (io_end.tv_nsec - io_start.tv_nsec);
        chunk_done_size += data->io_size;
        data->total_io_time += io_time;
        data->io_operations++;
        data->latency_vec.push_back(io_time);
    }
batch_destory:
    cuFileBatchIODestroy(batch_handle);
deregister_buffer:

    cuFileHandleDeregister(cf_handle);
    return NULL;

}

int run_gds(GDSOpts opts){
    struct timespec prog_start, prog_end;
    CUfileError_t status;
    GDSThread *threads;
    size_t chunk_size;
    std::vector<uint64_t> latency_vec;
    long long total_io_time = 0, prog_time;
    unsigned long long total_io_operations = 0;
    double average_io_latency, average_io_bandwidth;
    int file_fd;
    CUfileDescr_t cf_descr;
    CUfileHandle_t cf_handle;

    static void *(*rw_func[3][2])(void *arg) = {
        {read_thread, write_thread},
        {async_thread, async_thread},
        {batch_thread, batch_thread}};

    threads = new GDSThread[opts.num_threads];
    thread_prep(threads, opts.num_threads);

    file_fd = open(opts.file_path,  O_CREAT | O_RDWR | O_DIRECT, 0644);

    if (file_fd < 0) {
        perror("Open file error");
        return 1;
    }

    check_cudaruntimecall(cudaSetDevice(opts.gpu_id));

    status = cuFileDriverOpen();
    if (status.err != CU_FILE_SUCCESS) {
        std::cerr << "cufile driver open error: "
                << cuFileGetErrorString(status) << std::endl;
        exit(EXIT_FAILURE);
    }

    memset(&cf_descr, 0, sizeof(CUfileDescr_t));
    cf_descr.handle.fd = file_fd;
    cf_descr.type = CU_FILE_HANDLE_TYPE_OPAQUE_FD;
    status = cuFileHandleRegister(&cf_handle, &cf_descr);
    if (status.err != CU_FILE_SUCCESS) {
        std::cerr << "file register error:"
                    << cuFileGetErrorString(status) << std::endl;
        goto driver_close;
    }

    chunk_size = opts.length / opts.num_threads;

    for (int i = 0; i < opts.num_threads; i++){
        ThreadData *data = &threads[i].data;
        data->thread_id = i;
        data->offset = i * chunk_size;
        data->size = chunk_size;
        data->total_io_time = 0;
        data->io_operations = 0;
        data->device_id = opts.gpu_id;
        data->io_size = opts.io_size;
        data->depth = opts.io_depth;
        data->fd = file_fd;
        data->handler = &cf_handle;
        data->mode = opts.mode;


        if (opts.async == 0){
            check_cudaruntimecall(cudaMalloc(&data->gpu_buffer, data->size));
            check_cudaruntimecall(cudaMemset(data->gpu_buffer, 0x00, data->size));
            check_cudaruntimecall(cudaStreamSynchronize(0));

            status = cuFileBufRegister(data->gpu_buffer, data->size, 0);

            if (status.err != CU_FILE_SUCCESS) {
                std::cerr << "buffer register failed:"
                            << cuFileGetErrorString(status) << std::endl;
                return 1;
            }
        }else{
            unsigned long long  batch_chunk_size = data->size / data->depth;
            printf("batch_chunk_size: %llu\n", batch_chunk_size);
            data->gpu_buffers = new void*[data->depth];
            for (long unsigned int j = 0; j < data->depth; j++){
                check_cudaruntimecall(cudaMalloc(&data->gpu_buffers[j], batch_chunk_size));
                check_cudaruntimecall(cudaMemset((void*)(data->gpu_buffers[j]), 0xab, batch_chunk_size));
                check_cudaruntimecall(cudaStreamSynchronize(0));
                status = cuFileBufRegister(data->gpu_buffers[j], batch_chunk_size, 0);
                if (status.err != CU_FILE_SUCCESS) {
                    pr_error("buffer register failed:" << cuFileGetErrorString(status));
                    return 1;
                }
            }
        }

    }
    clock_gettime(CLOCK_MONOTONIC, &prog_start);
    for (int i = 0; i < opts.num_threads; i++) {
        pthread_create(&threads[i].thread, NULL, rw_func[opts.async][opts.mode], &threads[i].data);
    }
    
    for (int i = 0; i < opts.num_threads; i++) {
        pthread_join(threads[i].thread, NULL);
    }
    clock_gettime(CLOCK_MONOTONIC, &prog_end);
    prog_time = (prog_end.tv_sec - prog_start.tv_sec) * 1000000000LL + (prog_end.tv_nsec - prog_start.tv_nsec);


    total_io_time =  total_io_operations = 0;
    for (int i = 0; i < opts.num_threads; i++) {
        total_io_time += threads[i].data.total_io_time;
        total_io_operations += threads[i].data.io_operations;
        latency_vec.insert(latency_vec.end(), threads[i].data.latency_vec.begin(), threads[i].data.latency_vec.end());
    }
    pr_info("Total IO operations: " << total_io_operations);

    average_io_bandwidth = (total_io_operations * opts.io_size * opts.io_depth * 1.0/(MB)) / (prog_time / 1000000000.0);
    pr_info("Average IO bandwidth: " << average_io_bandwidth << " MB/s");


    average_io_latency = (double)total_io_time / (total_io_operations * 1000);
    pr_info("Average IO latency: " << average_io_latency << " ns");
    get_percentile(latency_vec);
    latency_vec.clear();

    for (int i =0;i<opts.num_threads;i++){
        ThreadData *data = &threads[i].data;
        if (opts.async ==0 ){
            if (data->gpu_buffer != NULL){
                // printf("free buffer %p\n", data->gpu_buffer);
                cuFileBufDeregister(data->gpu_buffer);
                cudaFree(data->gpu_buffer);
                data->gpu_buffer = NULL;
            }
        }else{
            for (size_t j =0;j<data->depth;j++){
                if (data->gpu_buffers[j] != NULL){
                    cuFileBufDeregister(data->gpu_buffers[j]);
                    check_cudaruntimecall(cudaFree(data->gpu_buffers[j]));
                }
               
            }
        }
    }

driver_close:
    cuFileHandleDeregister(cf_handle);
    return 0;
}
