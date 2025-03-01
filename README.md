# Billion Row Challenge

This project is an implementation of the Billion Row Challenge, which involves processing a large dataset of weather station measurements efficiently.

## Overview

The challenge is to process a file containing billions of weather station measurements in the format `station_name;temperature_value` and calculate the min, mean, and max temperature for each station.

## Project Structure

- `main.go` - The main processing program that reads and analyzes the data
- `generate_data.go` - A utility to generate test data of any size
- `data/` - Directory where the measurement data is stored

## Performance

The current implementation:

- Uses parallel processing with worker goroutines
- Processes data in batches for better memory efficiency
- Includes detailed timing metrics for performance analysis
- **Can process 1 billion records in under 17 seconds on a modern machine**

## Getting Started

### Prerequisites

- Go 1.16 or higher

### Generating Test Data

Generate a test file with 10 million records:

```bash
go run generate_data.go -n 10000000
```

You can adjust the number of records with the `-n` flag. For the full billion row challenge:

```bash
go run generate_data.go -n 1000000000
```

Note: Generating a billion records will take significant time and disk space (approximately 20-25GB).

### Running the Challenge

After generating the data file, run the main program:

```bash
go run main.go
```

The program will process the data and output:

- Timing information for each phase of processing
- The final result showing min/mean/max temperatures for each station

## Performance Optimization

The code includes several optimizations:

- Parallel processing using multiple CPU cores
- Efficient memory usage with batch processing
- Minimized string operations with byte-level parsing
- Memory-mapped file access for ultra-fast I/O
- Reusing slices to reduce memory allocations
- Detailed timing metrics to identify bottlenecks
- Optimized concurrency model with proper channel buffering
- Separate goroutines for file reading and result processing
- Dynamic buffer sizing based on file size
- Adaptive batch size for different file sizes
- Lock-free concurrent map for result aggregation

## Advanced Optimizations

The implementation includes several cutting-edge optimizations:

1. **Memory-Mapped File Access**: Instead of traditional file I/O, we use memory mapping (`mmap`) to allow the OS to efficiently manage file data in memory. This dramatically reduces system calls and buffer copying, resulting in much faster data access.

2. **Byte-Level Parsing**: We process data at the byte level using `bytes` package functions instead of string operations, which reduces memory allocations and garbage collection overhead.

3. **Lock-Free Concurrent Map**: We use Go's `sync.Map` for aggregating results, which is specifically designed for high-concurrency scenarios with minimal lock contention.

4. **Dynamic Batch Sizing**: Batch sizes are automatically adjusted based on file size, with larger files using larger batches (up to 8MB per batch) to reduce processing overhead.

5. **Two-Phase Aggregation**: Results are processed in parallel by multiple goroutines, with each handling a subset of the data to maximize CPU utilization.

## Latest Performance Results

### 1 Billion Records

```
Starting billion row challenge at: 2025-03-01T12:25:16-06:00
File opened with mmap in: 40.292µs
File size: 8793.81 MB
Using 10 CPU cores
Processing file in 1100 batches of ~8 MB each
Workers initialized in: 17.625µs
Reading complete: 1100 batches in 16.716501416s
All 1100 result batches processed in 16.777454417s
Output formatted for 180 stations in: 128.375µs
{...station data...}
Total processing time: 16.777804084s
Challenge completed in: 16.792802708s
```

### Performance Breakdown

Based on the 1 billion record run:

- File opening with mmap: ~40µs (negligible)
- File reading and parsing: ~16.72s (99.5% of total time)
- Result aggregation: ~0.06s (0.4% of total time)
- Output formatting: ~128µs (negligible)

This represents a **55% performance improvement** over our previous implementation, which took ~38 seconds to process the same dataset.

## Memory Efficiency

The implementation is designed to be memory-efficient:

1. **Memory-Mapped Files**: The OS handles paging file data in and out of memory as needed, reducing the application's memory footprint.
2. **Byte-Level Processing**: Working directly with bytes instead of strings reduces memory allocations.
3. **Batch Processing**: Data is processed in fixed-size batches to control memory usage.
4. **Efficient Data Structures**: Maps are pre-allocated with expected capacity and we use sync.Map for concurrent access.

## Concurrency Model

The implementation uses a sophisticated concurrency model:

1. A dedicated goroutine reads the file and sends batches to worker goroutines
2. Multiple worker goroutines (one per CPU core) process batches in parallel
3. Results are aggregated using a lock-free concurrent map
4. Proper channel buffering prevents deadlocks and ensures smooth data flow

## Further Improvements

Potential areas for further optimization:

- Custom memory pooling to reduce GC pressure
- SIMD instructions for parallel data processing
- Pre-allocation of byte slices for station names
- Custom hash map implementation optimized for this specific use case
- Streaming processing with fixed memory budget
- GPU acceleration for parsing and aggregation

## Contributing

Feel free to submit pull requests with performance improvements or additional features.
