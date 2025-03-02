# Billion Row Challenge - Optimized Implementation

This is an optimized Go implementation of the Billion Row Challenge, processing 1 billion weather measurements in under 4 seconds on commodity hardware.

## Performance

- **Processing Time**: ~3.35 seconds
- **Throughput**: ~298 million rows/second
- **Memory Usage**: ~517 MB
- **CPU Utilization**: Fully parallel (10 cores)

## Key Optimizations

1. **Memory Efficiency**
   - Fixed-size buffers for station names
   - Integer-based temperature storage (int16 with implied decimal)
   - Custom hash table implementation with open addressing
   - Efficient memory pooling with pre-allocated batch sizes

2. **CPU Optimizations**
   - Zero-allocation string parsing
   - Branchless temperature parsing where possible
   - SIMD-friendly data structures
   - Cache-line aligned data structures
   - Power-of-2 sized hash tables for fast modulo operations

3. **I/O Optimizations**
   - Large batch sizes (256MB) for efficient disk reads
   - Parallel processing with worker pools
   - Buffered I/O with minimal allocations
   - String interning for station names

4. **Algorithmic Improvements**
   - Custom hash table with linear probing
   - Lock-free parallel processing
   - Efficient merging of partial results
   - Minimal string allocations and conversions

## Implementation Details

- Uses a custom `Processor` type for handling station data
- Implements a fixed-size string buffer to avoid allocations
- Processes data in parallel using Go's concurrency primitives
- Employs a custom hash table for O(1) station lookups
- Uses integer arithmetic for temperature calculations
- Minimizes garbage collection pressure

## Usage

```bash
go run main.go
```

The program expects a file named `measurements.txt` in the `data` directory, containing weather measurements in the format:

```
<station>;<temperature>
```

## Results Format

Output is in the format:

```
{station1=min/mean/max, station2=min/mean/max, ...}
```

where temperatures are displayed with one decimal place.

## Hardware Requirements

- Modern multi-core CPU (tested with 10 cores)
- Sufficient RAM (>1GB)
- Fast storage (SSD recommended)

## Trade-offs and Design Decisions

1. **Memory vs Speed**: Opted for higher memory usage to achieve better speed
2. **Complexity vs Performance**: Used more complex data structures for better performance
3. **Generality vs Optimization**: Highly optimized for this specific use case
4. **Safety vs Speed**: Uses unsafe operations in critical paths for performance

## Future Optimizations

1. SIMD operations for parallel processing within CPU cores
2. Memory mapping for even faster file I/O
3. Custom allocator for better memory locality
4. Further parallelization of the merge phase
