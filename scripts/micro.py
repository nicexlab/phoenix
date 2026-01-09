import os
import subprocess
import sys
import re
import time
import shlex

from logger import Log

result_path = ""
FILE_PATH = "/mnt/phxfs/test.data"
SUBDIR = "phxfs"
# 1M 
MB = 1024
io_sizes = [4, 8, 16, 32, 64, 128, 256, 512, 1024]
threads = [1, 2, 4, 8, 16, 32, 64, 128]
batch_sizes = [1, 2, 4, 8, 16, 32, 64, 128, 256]

read_write = ["read", "write"]
file_path = os.path.dirname(os.path.realpath(__file__))
micro_exec = os.path.join(file_path, "..", "build", "bin", "microbenchmark")

class test_config:
    def __init__(self):
        self.muti_size = False
        self.muti_thread = False
        self.muti_batch = False
        self.async_mode = 0
        self.xfer_mode = 0
    
    def reset(self):
        self.muti_size = False
        self.muti_thread = False
        self.muti_batch = False

pattern = r"(?:Average IO bandwidth|Average IO latency|95th percentile latency|99th percentile latency|99.9th percentile latency):\s*([\d.]+)"

def run_bench(rw="read", io_size=4, thread=1, batch_size=16, file_path_=FILE_PATH, async_mode=0, xfer_mode=0):
    if batch_size > 64:
        return f"{micro_exec} -f {file_path_} -l 10G -s {io_size}k -t {thread} -i {batch_size} -m {rw} -a {async_mode} -d 0 -x {xfer_mode}"
    return f"numactl -N 0 {micro_exec} -f {file_path_} -l 10G -s {io_size}k -t {thread} -i 1 -m {rw} -a {async_mode} -d 0 -x {xfer_mode}"

def parse_result(result):
    matches = re.findall(pattern, result)
    matches = [float(match) for match in matches]
    return matches[0], matches[1], matches[2], matches[3], matches[4]

def x_thread_y_size_z_batch(config: test_config):
    f = open(result_path, "a+")
    print(f"result_path: {result_path}")
    io_size_iter = io_sizes if config.muti_size == True else [4]
    thread_iter = threads if config.muti_thread  == True else [1]
    batch_size_iter = batch_sizes if config.muti_batch == True else [1]
    f.write("thread-rw-io_size-batch_size,bandwidth,latency,p95_latency,p99_latency,p999_latency\n")
    f.write(f"async_mode: {config.async_mode}, xfer_mode: {config.xfer_mode}\n")
    for rw in read_write:
        for io_size in io_size_iter:
            for thread in thread_iter:
                for batch_size in batch_size_iter:
                    # subprocess.run("echo 3 | sudo tee /proc/sys/vm/drop_caches", shell=True)
                    cmdline = run_bench(rw=rw, io_size=io_size, thread=thread, batch_size=batch_size, async_mode=config.async_mode, xfer_mode=config.xfer_mode, file_path_=FILE_PATH)
                    Log.info(f"Run {cmdline}")
                    result = subprocess.check_output(cmdline, shell=True).decode()
                    Log.info(result)
                    bandwidth, latency, p95_latency, p99_latency, p999_latency = parse_result(result)
                    f.write(f"{thread}-{rw}-{io_size}-{batch_size},{bandwidth},{latency},{p95_latency},{p99_latency},{p999_latency}\n")
                    f.flush()

def run_perf_cpu(pid: int):
    return subprocess.Popen("")


if __name__ == "__main__":
    if len(sys.argv) != 5:
        Log.error("Usage: python run_batch.py <xfer_mode> <mode> <device_type> <file_path>")
        Log.info("xfer_mode: phxfs, gds")
        Log.info("mode: sync, async, batch")
        Log.info("device_type: 0 - nvme, 1 - nvmeof")
        sys.exit(1)

    xfer_mode_str  = sys.argv[1].lower()     # phxfs / gds
    run_mode_str   = sys.argv[2].lower()     # sync / async / batch
    device_type_str = sys.argv[3].lower()    # 0 - nvme / 1 - nvmeof
    FILE_PATH = sys.argv[4]

    xfer_mode_map  = {"phxfs": 0, "gds": 1, "posix": 2}
    run_mode_map   = {"sync": 0, "async": 1, "batch": 2}
    device_type_map = { "nvme": "0", "nvmeof": "1" }

    
    if xfer_mode_str not in xfer_mode_map:
        Log.error("Invalid xfer_mode. Must be 'phxfs' or 'gds' or 'posix'.")
        sys.exit(1)
    if run_mode_str not in run_mode_map:
        Log.error("Invalid mode. Must be 'sync', 'async' or 'batch'.")
        sys.exit(1)
    if device_type_str not in device_type_map:
        Log.error("Invalid device type. Must be 0 (nvme) or 1 (nvmeof).")
        sys.exit(1)

    xfer_mode   = xfer_mode_map[xfer_mode_str]   # 0 / 1 / 2
    async_mode  = run_mode_map[run_mode_str]     # 0 / 1 / 2
    device_type = device_type_map[device_type_str]           # 0 / 1 
    
    result_dir = os.path.join(file_path, "results", "latency", xfer_mode_str)
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)

    result_path = os.path.join(result_dir, f"{run_mode_str}_{device_type_str}.txt")

    config = test_config()
    config.reset()
    config.async_mode = async_mode
    config.xfer_mode = xfer_mode
    
    # change the config to test different modes
    # for example, config.muti_thread indicates that multi-threading testing will be conducted
    config.muti_size = True
    config.muti_thread = True
    x_thread_y_size_z_batch(config)
    config.reset()
