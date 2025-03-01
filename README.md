# Billion Row Challenge

This project is an implementation of the Billion Row Challenge, which involves processing a large dataset of weather station measurements efficiently.

## Overview

The challenge is to process a file containing billions of weather station measurements in the format `station_name;temperature_value` and calculate the min, mean, and max temperature for each station.

## Project Structure

- `main.go` - The main processing program that reads and analyzes the data
- `data/generate_data.go` - A utility to generate test data of any size
- `data/` - Directory where the measurement data is stored

## Performance

The current implementation:

- Uses parallel processing with worker goroutines
- Processes data in batches for better memory efficiency
- Includes detailed timing metrics for performance analysis
- **Can process 1 billion records in under 15 seconds on a modern machine**

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

## Performance Comparison

| Optimization Approach                         | Processing Time | Improvement |
| --------------------------------------------- | --------------- | ----------- |
| Initial Implementation (Buffered Scanner)     | 38.0 seconds    | Baseline    |
| Memory-Mapped Files + Byte-Level Parsing      | 16.8 seconds    | 55.8%       |
| + Custom Hash Table + Zero-Allocation Parsing | 15.0 seconds    | 60.5%       |

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
- Custom hash table implementation for efficient station data storage

## Advanced Optimizations

The implementation includes several cutting-edge optimizations:

1. **Memory-Mapped File Access**: Instead of traditional file I/O, we use memory mapping (`mmap`) to allow the OS to efficiently manage file data in memory. This dramatically reduces system calls and buffer copying, resulting in much faster data access.

2. **Byte-Level Parsing**: We process data at the byte level instead of string operations, which reduces memory allocations and garbage collection overhead.

3. **Custom Hash Table**: We implement a custom hash table optimized for station names using open addressing with linear probing, which is more efficient than Go's built-in maps for our specific use case.

4. **Zero-Allocation Parsing**: We parse floating-point values directly from byte slices without allocations, avoiding the overhead of standard library functions.

5. **Unsafe String Conversion**: We convert byte slices to strings without allocations using unsafe pointers, which significantly reduces memory pressure.

6. **Dynamic Batch Sizing**: Batch sizes are automatically adjusted based on file size, with larger files using larger batches (up to 16MB per batch) to reduce processing overhead.

7. **Two-Phase Aggregation**: Results are processed in parallel by multiple goroutines, with each handling a subset of the data to maximize CPU utilization.
