#include <cstddef>
#include <pthread.h>
#include <sys/types.h>
#include <unistd.h>
#include <liburing.h>
#include <cuda.h>
#include <pthread.h>
#include "phxfs_utils.h"
#include <fcntl.h> 

static void *write_thread(void *arg) {
    struct timespec io_start, io_end;
    ThreadData *data = (ThreadData *)arg;
    size_t written = 0;
    ssize_t io_size = (ssize_t)data->io_size;
    int fd = data->fd;
    uint64_t io_time = 0;
    pr_info(__func__);
    while (written < data->size) {
        
        clock_gettime(CLOCK_MONOTONIC, &io_start);
        check_cudaruntimecall(cudaMemcpy(
            point_offset(data->buffer, written), 
            point_offset(data->gpu_buffer, data->offset + written),
            data->io_size, cudaMemcpyDeviceToHost));
        check_cudaruntimecall(cudaStreamSynchronize(0));
        ssize_t result = pwrite(fd, 
            point_offset(data->buffer, written),
            data->io_size, data->offset + written);
        if (result == 0) {
            // End of file reached
            break;
        }
        if (result != io_size) {
            std::cerr << "write_thread error, result is " << result << ", size is " << data->io_size << std::endl;
            return NULL;
        }
        clock_gettime(CLOCK_MONOTONIC, &io_end);
        io_time = (io_end.tv_sec - io_start.tv_sec) * 1000000000LL + (io_end.tv_nsec - io_start.tv_nsec);
        data->total_io_time += io_time;
        data->io_operations++;
        data->latency_vec.push_back(io_time);
        written += result;
    }
    clock_gettime(CLOCK_MONOTONIC, &data->end_time);
    return NULL;
}

static void *read_thread(void *arg) {
    struct timespec io_start, io_end;
    ThreadData *data = (ThreadData *)arg;
    size_t read_bytes = 0;
    ssize_t result;
    ssize_t io_size = (ssize_t)data->io_size;
    uint64_t io_time = 0;
    int fd = data->fd;
    pr_info(__func__);
    while (read_bytes < data->size) {
        clock_gettime(CLOCK_MONOTONIC, &io_start); 
        result = pread(fd, 
            data->buffer,
            data->io_size, data->offset + read_bytes);
        if (result == 0) {
            // End of file reached
            break;
        }
        if (result != io_size) {
            std::cerr << "read_thread error, result is " << result << ", size is " << data->io_size << std::endl;
            return NULL;
        }
        check_cudaruntimecall(cudaMemcpy(
            data->gpu_buffer,
            data->buffer,
            data->io_size, cudaMemcpyHostToDevice));
        check_cudaruntimecall(cudaStreamSynchronize(0));
        clock_gettime(CLOCK_MONOTONIC, &io_end);
        io_time = (io_end.tv_sec - io_start.tv_sec) * 1000000000LL + (io_end.tv_nsec - io_start.tv_nsec);
        data->total_io_time += io_time;
        data->io_operations++;
        data->latency_vec.push_back(io_time);
        read_bytes += result;
    }
    clock_gettime(CLOCK_MONOTONIC, &data->end_time);
    return NULL;
}

int run_posix(GDSOpts opts){
    GDSThread *threads;
    size_t chunk_size;
    unsigned long long total_io_operations = 0, total_io_time = 0;
    double average_io_latency, average_io_bandwidth;
    int file_fd, ret;
    struct timespec prog_start, prog_end;
    unsigned long long prog_time = 0;
    std::vector<uint64_t> latency_vec;


    static void *(*rw_funcs[2][2])(void *arg) = {{read_thread, write_thread}, {NULL, NULL}};


    threads = new GDSThread[opts.num_threads];
    thread_prep(threads, opts.num_threads);

    file_fd = open(opts.file_path,  O_CREAT | O_RDWR | O_DIRECT, 0644);

    if (file_fd < 0) {
        perror("Open file error");
        return 1;
    }

    check_cudaruntimecall(cudaSetDevice(opts.gpu_id));


    chunk_size = opts.length / opts.num_threads;

    for (int i = 0; i < opts.num_threads; i++){
        ThreadData *data = &threads[i].data;
        data->thread_id = i;
        data->offset = i * chunk_size;
        data->size = (i == opts.num_threads - 1) ? (opts.length - i * chunk_size) : chunk_size;
        data->total_io_time = 0;
        data->io_operations = 0;
        data->fd = file_fd;
        data->device_id = opts.gpu_id;
        data->io_size = opts.io_size;
        data->depth = opts.io_depth;
        data->mode = opts.mode;

        // 分块注册
        check_cudaruntimecall(cudaMalloc(&data->gpu_buffer, data->size));
        check_cudaruntimecall(cudaMemset(data->gpu_buffer, 0x00, data->size));
        check_cudaruntimecall(cudaStreamSynchronize(0));

        ret = posix_memalign(&data->buffer, 4096, data->size);
        if (ret != 0) {
            data->buffer = NULL;
            pr_error("buffer alloc error");
            goto out;
        }

        if (opts.async > 0){
            goto out;
        }

    }
    clock_gettime(CLOCK_MONOTONIC, &prog_start);
    for (int i = 0; i < opts.num_threads; i++) {
        if (pthread_create(&threads[i].thread, NULL, rw_funcs[opts.async][opts.mode], &threads[i].data) != 0) {
            pr_error("Pthread create error");
            goto out;
        }
    }
    for (int i = 0; i < opts.num_threads; i++) {
        pthread_join(threads[i].thread, NULL);
    }  
    clock_gettime(CLOCK_MONOTONIC, &prog_end);
    prog_time = (prog_end.tv_sec - prog_start.tv_sec) * 1000000000LL + (prog_end.tv_nsec - prog_start.tv_nsec);

    for (int i = 0; i < opts.num_threads; i++) {
        total_io_time += threads[i].data.total_io_time;
        total_io_operations += threads[i].data.io_operations;
        latency_vec.insert(latency_vec.end(), threads[i].data.latency_vec.begin(), threads[i].data.latency_vec.end());
    }
    pr_info("Total IO operations: " << total_io_operations);
    average_io_bandwidth = (((double)total_io_operations * opts.io_size * opts.io_depth)/(MB) ) / (1.0 * prog_time / 1000000000.0);
    pr_info("Average IO bandwidth: " << average_io_bandwidth << " MB/s");
    average_io_latency = (double)total_io_time / (total_io_operations * 1000.0);
    pr_info("Average IO latency: " << average_io_latency << " us");
    get_percentile(latency_vec);
out:
    for (int i = 0; i < opts.num_threads; i++) {
        if (threads[i].data.buffer)
            free(threads[i].data.buffer);
        if (threads[i].data.gpu_buffer)
            check_cudaruntimecall(cudaFree(threads[i].data.gpu_buffer));
    }
    close(file_fd);
    return 0;
}

