# Billion Row Challenge Benchmarking

This directory contains tools for benchmarking the different implementations of the Billion Row Challenge.

## Available Implementations

1. **V1**: Original implementation with basic optimizations
2. **V2**: Highly optimized implementation with custom data structures
3. **V3**: Alternative implementation with different file splitting approach

## Running Benchmarks

### Using the Go Benchmark Runner

The easiest way to run benchmarks is using the Go benchmark runner:

```bash
go run run_benchmarks.go [iterations]
```

Where `iterations` is an optional parameter specifying how many times to run each implementation (default: 5).

### Using the Shell Script

Alternatively, you can use the shell script:

```bash
chmod +x run_benchmark.sh
./run_benchmark.sh [iterations]
```

## Benchmark Results

The benchmark results will be displayed in the console and also saved to a file named `benchmark_results.txt`.

The results include:

- Average execution time
- Median execution time
- Min/max execution times
- Memory usage
- Throughput (million rows/second)
- Speedup comparisons between implementations

## Example Output

```
=== BENCHMARK RESULTS ===
Date: Mon, 01 Jan 2024 12:00:00 UTC
CPU: 10 cores
OS: darwin

Version: V1
  Average Time: 14.5s
  Median Time:  14.3s
  Min Time:     14.1s
  Max Time:     15.2s
  Memory Used:  1024.50 MB
  Throughput:   68.97 million rows/second

Version: V2
  Average Time: 3.5s
  Median Time:  3.4s
  Min Time:     3.3s
  Max Time:     3.8s
  Memory Used:  512.75 MB
  Throughput:   285.71 million rows/second

Version: V3
  Average Time: 5.2s
  Median Time:  5.1s
  Min Time:     5.0s
  Max Time:     5.5s
  Memory Used:  768.30 MB
  Throughput:   192.31 million rows/second

Fastest implementation: V2 (285.71 million rows/second)

Speedup Comparisons:
  V1 → V2: 4.14x
  V1 → V3: 2.79x
  V2 → V3: 0.67x
```

## Customizing Benchmarks

To customize the benchmarks, you can modify the following files:

- `run_benchmarks.go`: The main benchmark runner
- `run_benchmark.sh`: Shell script for running benchmarks

## Notes

- The benchmarks run each implementation multiple times to get more accurate results
- Between runs, garbage collection is forced and a small delay is added to stabilize the system
- Memory usage is measured using Go's runtime memory statistics
- Throughput is calculated assuming 1 billion rows in the dataset
