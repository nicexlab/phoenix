#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <sys/types.h>
#include <unistd.h>
#include <ctime>
#include <atomic>
#include <errno.h>
#include <vector>
#include <sys/stat.h>

#include "phoenix.h"
#include <liburing.h>
#include <liburing/io_uring.h>

extern "C" {
#include "fio.h"
#include "optgroup.h"
}

static std::atomic<bool> driver_inited{false};

struct nds_options {
    void *pad;
    int dummy;
    int enable_nds;
    int device_id;
    int enable_iouring;
};

static struct fio_option options[] = {
    {
        .name       = "enable_iouring",
        .lname      = "Enable io_uring backend",
        .type       = FIO_OPT_INT,
        .off1       = offsetof(struct nds_options, enable_iouring),
        .help       = "Set to 1 to enable io_uring backend",
        .def        = "0",
        .category = FIO_OPT_C_ENGINE,
        .group = FIO_OPT_G_NETIO,
    },
    {
        .name       = "enable_nds",
        .lname      = "Enable NDS optimizations",
        .type       = FIO_OPT_INT,
        .off1       = offsetof(struct nds_options, enable_nds),
        .help       = "Set to 1 to enable NDS optimizations",
        .def        = "0",
        .category = FIO_OPT_C_ENGINE,
        .group = FIO_OPT_G_NETIO,
    },
    {
        .name       = "device_id",
        .lname      = "NDS Device ID",
        .type       = FIO_OPT_INT,
        .off1       = offsetof(struct nds_options, device_id),
        .help       = "Set the NDS device ID to use",
        .def        = "0",
        .category = FIO_OPT_C_ENGINE,
        .group = FIO_OPT_G_NETIO,
    },
    {NULL}
};


#define LAST_POS(f) ((f)->engine_pos)

struct nds_data {
    struct io_uring *read_ring;
    struct io_uring *write_ring;
    void *dev_buf;
    void *host_buf;
    size_t buf_size;
    std::vector<struct io_u *> io_us;
    int queued;
    int events;
    enum fio_ddir last_ddir;
};


static int nds_init(struct thread_data *td) {
    td->io_ops_data = static_cast<void *>(new nds_data);
    struct nds_options * opts = (struct nds_options *)td->eo;
    auto data = static_cast<nds_data *>(td->io_ops_data);
    data->last_ddir = DDIR_READ;

    if (opts->enable_nds) {
        if (!driver_inited) {
            driver_inited = true;
            if (phxfs_open(opts->device_id) != 0) {
                std::cerr << "Failed to initialize Phoenix driver" << std::endl;
                return -EIO;
            }
        }
    }

    std::cout << "nds_init: enable_nds=" << opts->enable_nds
              << ", enable_iouring=" << opts->enable_iouring
              << ", device_id=" << opts->device_id << std::endl;

    if (opts->enable_nds && opts->enable_iouring) {
        std::cout << "Using NDS io_uring backend" << std::endl;

        data->read_ring = new io_uring();
        data->write_ring = new io_uring();

        struct io_uring_params params; 
        memset(&params, 0, sizeof(params));
        params.flags = 0;
        params.cq_entries = params.sq_entries = td->o.iodepth;

        std::cout << "Initializing io_uring with iodepth=" << td->o.iodepth << std::endl;
        if (io_uring_queue_init_params(td->o.iodepth, data->read_ring, &params)) {
            std::cerr << "io_uring_queue_init_params read failed" << std::endl;
            return -ENOMEM;
        }
        if (io_uring_queue_init_params(td->o.iodepth, data->write_ring, &params)) {
            std::cerr << "io_uring_queue_init_params write failed" << std::endl;
            return -ENOMEM;
        }
        
    } else {
        data->read_ring = nullptr;
        data->write_ring = nullptr;
    }

    data->io_us.resize(td->o.iodepth);
    data->queued = 0;
    data->events = 0;
    return 0;
}

static int fio_io_end(struct thread_data *td, struct io_u *io_u, int ret) {
    if (io_u->file && ret >= 0 && ddir_rw(io_u->ddir)) {
        LAST_POS(io_u->file) = io_u->offset + ret;
    }

    if (ret != (int) io_u->xfer_buflen) {
        if (ret >= 0) {
            io_u->resid = io_u->xfer_buflen - ret;
            io_u->error = 0;
            return FIO_Q_COMPLETED;
        } else {
            io_u->error = errno;
        }
    }

    if (io_u->error) {
        io_u_log_error(td, io_u);
        td_verror(td, io_u->error, "xfer");
    }

    return FIO_Q_COMPLETED;
}

static enum fio_q_status nds_queue(struct thread_data *td, struct io_u *io_u) {
    auto &vec = static_cast<nds_data *>(td->io_ops_data)->io_us;
    auto *sd = static_cast<nds_data *>(td->io_ops_data);

    if (io_u->ddir != sd->last_ddir) {
        if (sd->queued != 0) {
            return FIO_Q_BUSY;
        } else {
            vec[sd->queued++] = io_u;
            sd->last_ddir = io_u->ddir;
            return FIO_Q_QUEUED;
        }
    } else {
        if (sd->queued == td->o.iodepth) {
            return FIO_Q_BUSY;
        }
        vec[sd->queued++] = io_u;
        return FIO_Q_QUEUED;
    }
}

static int nds_iouring_commit(struct nds_options *opts, nds_data *sd) {
    bool read = (sd->last_ddir == DDIR_READ);
    auto &vec = sd->io_us;
    auto ring = read ? sd->read_ring : sd->write_ring;
    int res;

    size_t total_submitted = 0;
    for (int i = 0; i < sd->queued; i++) {
        struct io_u *io_u = vec[i];
        auto buf_offset = (uint64_t)io_u->xfer_buf - (uint64_t)sd->host_buf;
        phxfs_xfer_addr *xfer_addr = NULL;

        xfer_addr = phxfs_do_xfer_addr(opts->device_id, sd->dev_buf, buf_offset, io_u->xfer_buflen);
        if (!xfer_addr) {
            std::cerr << "phxfs_do_xfer_addr failed" << std::endl;
            return -EIO;
        }

        size_t internal_bytes = 0;
        for (int j = 0; j < xfer_addr->nr_xfer_addrs; j++) {
            struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
            if (!sqe) {
                std::cerr << "io_uring_get_sqe failed" << std::endl;
                return -ENOMEM;
            }
            io_uring_prep_rw(
                read ? IORING_OP_READ : IORING_OP_WRITE,
                sqe,
                io_u->file->fd,
                (char *)xfer_addr->x_addrs[j].target_addr,
                xfer_addr->x_addrs[j].nbyte,
                io_u->offset + internal_bytes
            );
            internal_bytes += xfer_addr->x_addrs[j].nbyte;
            total_submitted ++;   
        }

        if (internal_bytes != io_u->xfer_buflen) {
            std::cerr << "internal_bytes != xfer_buflen" << std::endl;
            return -EIO;
        }
    }
    res = io_uring_submit(ring);
    if (res < 0) {
        std::cerr << "io_uring_submit failed: " << strerror(-res) << std::endl;
        return res;
    }

    int nr_completions = 0;
    while (nr_completions < total_submitted) {
        struct io_uring_cqe *cqe;
        res = io_uring_wait_cqe(ring, &cqe);
        if (res < 0) {
            std::cerr << "io_uring_wait_cqe failed: " << strerror(-res) << std::endl;
            return res;
        }
        io_uring_cqe_seen(ring, cqe);
        nr_completions++;
    }
        
   return 0;
}

static int nds_sync_commit(struct nds_options *opts, nds_data *sd) {
    bool read = (sd->last_ddir == DDIR_READ);
    auto &vec = sd->io_us;
    int res;

    auto ioOp = read ? phxfs_read : phxfs_write;
    for (int i = 0; i < sd->queued; i++) {
        struct io_u *io_u = vec[i];
        auto buffer_offset = (uint64_t)io_u->xfer_buf - (uint64_t)sd->host_buf;
        res = ioOp(
            {io_u->file->fd, opts->device_id},
            sd->dev_buf,
            buffer_offset,
            io_u->xfer_buflen,
            io_u->offset
        );
        if (res < 0) {
            std::cerr << "phxfs_read/phxfs_write failed: " << strerror(-res) << std::endl;
            return res;
        }
    }
    return 0;
}

static int nds_native_commit(struct nds_options *opts, nds_data *sd) {
    bool read = (sd->last_ddir == DDIR_READ);
    auto &vec = sd->io_us;
    int res;

    for (int i = 0; i < sd->queued; i++) {
        struct io_u *io_u = vec[i];
        auto dev_ptr = (void *)((uint64_t)io_u->xfer_buf - (uint64_t)sd->host_buf + (uint64_t)sd->dev_buf);

        res = read ? pread(io_u->file->fd, io_u->xfer_buf, io_u->xfer_buflen, io_u->offset)
                   : pwrite(io_u->file->fd, io_u->xfer_buf, io_u->xfer_buflen, io_u->offset);
        
        if (res < 0) {
            std::cerr << "pread/pwrite failed: " << strerror(errno) << std::endl;
            return -errno;
        }

        auto err = read ? cudaMemcpy(dev_ptr, io_u->xfer_buf, io_u->xfer_buflen, cudaMemcpyHostToDevice)
                                     : cudaMemcpy(io_u->xfer_buf, dev_ptr, io_u->xfer_buflen, cudaMemcpyDeviceToHost);
        if (err != cudaSuccess) {
            std::cerr << "cudaMemcpy failed: " << cudaGetErrorString(err) << std::endl;
            return -EIO;
        }
    }
    
    return 0;
}


static int nds_commit(struct thread_data *td) {
    auto sd = static_cast<nds_data *>(td->io_ops_data);
    auto &vec = sd->io_us;
    auto opts = (struct nds_options *)td->eo;

    if (sd->queued == 0) {
        return 0;
    }

    io_u_mark_submit(td, sd->queued);
    int res = 0;
    if (opts->enable_nds && opts->enable_iouring)
        res = nds_iouring_commit(opts, sd);
    
    if (opts->enable_nds && !opts->enable_iouring)
        res = nds_sync_commit(opts, sd);

    if (!opts->enable_nds)
        res = nds_native_commit(opts, sd);

    if (res < 0) {
        std::cerr << "Commit failed" << std::endl;
        return res;
    }

    sd->events = sd->queued;
    sd->queued = 0;

    return 0;
}

static int nds_getevents(struct thread_data *td, unsigned int min, unsigned int max, const struct timespec *ts) {
    auto &vec = static_cast<nds_data *>(td->io_ops_data)->io_us;
    auto *sd = static_cast<nds_data *>(td->io_ops_data);
    int ret = 0;
    if (min) {
        ret = sd->events;
        sd->events = 0;
    }

    return ret;
}

static struct io_u *nds_event(struct thread_data *td, int event) {
    auto &vec = static_cast<nds_data *>(td->io_ops_data)->io_us;
    return vec[event];
}

static void nds_cleanup(struct thread_data *td) {
    auto opts = (struct nds_options *)td->eo;
    if (opts->enable_nds && driver_inited) {
        phxfs_close(opts->device_id);
        driver_inited = false;
    }

    if (opts->enable_nds && opts->enable_iouring) {
        auto data = static_cast<nds_data *>(td->io_ops_data);
        if (data->read_ring) {
            io_uring_queue_exit(data->read_ring);
            delete data->read_ring;
        }
        
        if (data->write_ring) {
            io_uring_queue_exit(data->write_ring);
            delete data->write_ring;
        }
    }

    delete static_cast<nds_data *>(td->io_ops_data);
}

static int nds_file_open(struct thread_data *td, struct fio_file *f) {
    int flags = 0;
    if (td_write(td)) {
        if (!read_only) {
            flags = O_RDWR;
        }
    } else if (td_read(td)) {
        if (!read_only) {
            flags = O_RDWR;
        } else {
            flags = O_RDONLY;
        }
    }

    f->fd = open(f->file_name, flags | O_DIRECT);
    std::cout << "nds open file: " << f->file_name << " fd: " << f->fd << std::endl;
    td->o.open_files++;
    return 0;
}

static int nds_file_close(struct thread_data *td, struct fio_file *f) {
    close(f->fd);
    f->fd = -1;
    return 0;
}

static int nds_buffer_alloc(struct thread_data *td, size_t total_mem) {
    struct nds_options *options = static_cast<nds_options *>(td->eo);
    auto data = static_cast<nds_data *>(td->io_ops_data);
    auto &dev_buf = data->dev_buf;
    auto &host_buf = data->host_buf;

    // force align to 64KB
    if (total_mem % (64 * 1024) != 0) {
        total_mem = ((total_mem / (64 * 1024)) + 1) * (64 * 1024);
    }

    data->buf_size = total_mem;
    cudaError_t err = cudaMalloc(&dev_buf, total_mem);
    if (err != cudaSuccess) {
        std::cerr << "cudaMalloc failed: " << cudaGetErrorString(err) << std::endl;
        return -ENOMEM;
    }
    if (options->enable_nds) {
        void *host_ptr = nullptr;
        if (phxfs_regmem(options->device_id, dev_buf, total_mem, &host_ptr) != 0) {
            std::cerr << "phxfs_regmem failed" << std::endl;
            return -ENOMEM;
        }
        host_buf = dev_buf;
    } else {
        err = cudaMallocHost(&host_buf, total_mem);
        if (err != cudaSuccess) {
            std::cerr << "cudaMallocHost failed: " << cudaGetErrorString(err) << std::endl;
            return -ENOMEM;
        }
    }

    td->orig_buffer = (char *)host_buf;
    return 0;
}

static void nds_buffer_free(struct thread_data *td) {
    auto option = (struct nds_options *)td->eo;
    auto data = static_cast<nds_data *>(td->io_ops_data);
    auto &dev_buf = data->dev_buf;
    auto &host_buf = data->host_buf;

    if (option->enable_nds) {
        phxfs_deregmem(option->device_id, dev_buf, data->buf_size);
    } else {
        cudaFreeHost(host_buf);
        
    }
    cudaFree(dev_buf);
    host_buf = nullptr;
    dev_buf = nullptr;
    td->orig_buffer = nullptr;
}

static int nds_invalidate(struct thread_data *td, struct fio_file *f) {
    return 0;
}


extern "C" {
struct ioengine_ops ioengine = {
    .name               = "nds_ioengine",
    .version            = FIO_IOOPS_VERSION,
    .flags              = FIO_NODISKUTIL,
    .init               = nds_init,
    .queue              = nds_queue,
    .commit             = nds_commit,
    .getevents          = nds_getevents,
    .event              = nds_event,
    .cleanup            = nds_cleanup,
    .open_file          = nds_file_open,
    .close_file         = nds_file_close,
    .invalidate         = nds_invalidate,
    .get_file_size      = generic_get_file_size,
    .iomem_alloc        = nds_buffer_alloc,
    .iomem_free         = nds_buffer_free,
    .option_struct_size = sizeof(struct nds_options),
    .options            = options,
};

void get_ioengine(struct ioengine_ops **ioengine_ptr) {
    *ioengine_ptr = &ioengine;
}

}
