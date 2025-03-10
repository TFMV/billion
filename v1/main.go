package main

import (
	"fmt"
	"io"
	"log"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"golang.org/x/exp/mmap"
)

// Constants for optimization
const (
	BatchSize        = 8 * 1024 * 1024 // 8MB batch size for processing
	ExpectedStations = 10000           // Expected number of unique stations
)

// StationID is a numeric identifier for a station
type StationID uint16

// StationStats holds the statistics for a station
type StationStats struct {
	Min   float64
	Max   float64
	Sum   float64
	Count int64
}

// StationMap is a custom hash table for storing station statistics
// Uses open addressing with linear probing for better performance
type StationMap struct {
	keys     []string       // Station names
	values   []StationStats // Station statistics
	occupied []bool         // Tracks which slots are occupied
	size     int            // Current number of entries
	capacity int            // Total capacity
	mask     int            // Bit mask for fast modulo (capacity - 1)
}

// NewStationMap creates a new station map with the given capacity
// Capacity is rounded up to the next power of two for efficient hashing
func NewStationMap(capacity int) *StationMap {
	capacity = nextPowerOfTwo(capacity)
	return &StationMap{
		keys:     make([]string, capacity),
		values:   make([]StationStats, capacity),
		occupied: make([]bool, capacity),
		size:     0,
		capacity: capacity,
		mask:     capacity - 1,
	}
}

// nextPowerOfTwo returns the next power of two greater than or equal to v
func nextPowerOfTwo(v int) int {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v++
	return v
}

// hash computes a fast hash of the string
// Uses FNV-1a hash algorithm for good distribution
func hash(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// Get retrieves the statistics for a station
// Returns a pointer to the stats and a boolean indicating if found
func (m *StationMap) Get(station string) (*StationStats, bool) {
	h := hash(station)
	i := h & uint32(m.mask)
	for m.occupied[i] {
		if m.keys[i] == station {
			return &m.values[i], true
		}
		i = (i + 1) & uint32(m.mask)
	}
	return nil, false
}

// Put adds or updates statistics for a station
// Grows the map if it becomes too full (>75% capacity)
func (m *StationMap) Put(station string, stats StationStats) {
	if m.size >= m.capacity*3/4 {
		m.grow()
	}

	h := hash(station)
	i := h & uint32(m.mask)
	for m.occupied[i] {
		if m.keys[i] == station {
			m.values[i] = stats
			return
		}
		i = (i + 1) & uint32(m.mask)
	}

	m.keys[i] = station
	m.values[i] = stats
	m.occupied[i] = true
	m.size++
}

// grow increases the capacity of the map
// Creates a new map with double the capacity and rehashes all entries
func (m *StationMap) grow() {
	newMap := NewStationMap(m.capacity * 2)
	for i := 0; i < m.capacity; i++ {
		if m.occupied[i] {
			newMap.Put(m.keys[i], m.values[i])
		}
	}
	*m = *newMap
}

// Range iterates over all entries in the map
// Calls the provided function for each entry
func (m *StationMap) Range(f func(station string, stats *StationStats) bool) {
	for i := 0; i < m.capacity; i++ {
		if m.occupied[i] {
			if !f(m.keys[i], &m.values[i]) {
				return
			}
		}
	}
}

// ByteSlice is a slice of bytes for efficient parsing
type ByteSlice []byte

// ParseFloat parses a float from a byte slice
// Optimized for the specific format of temperatures in the dataset
func ParseFloat(b ByteSlice) (float64, error) {
	neg := false
	if b[0] == '-' {
		neg = true
		b = b[1:]
	}

	// Fast path for common cases
	var val float64
	var dot int = -1

	for i, c := range b {
		if c == '.' {
			dot = i
			continue
		}
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("invalid character in float: %c", c)
		}
		val = val*10 + float64(c-'0')
	}

	// Apply decimal point
	if dot >= 0 {
		val /= float64(pow10(len(b) - dot - 1))
	}

	if neg {
		val = -val
	}

	return val, nil
}

// pow10 returns 10^n
func pow10(n int) int {
	result := 1
	for i := 0; i < n; i++ {
		result *= 10
	}
	return result
}

// UnsafeString converts a byte slice to a string without copying
// This is unsafe but significantly faster for read-only operations
func UnsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// LineProcessor processes a batch of lines
// Updates the station map with statistics for each measurement
func LineProcessor(batch []byte, result *StationMap) {
	start := 0
	for i := 0; i < len(batch); i++ {
		if batch[i] == '\n' {
			line := batch[start:i]
			if len(line) > 0 {
				// Find the separator
				sepIdx := -1
				for j := 0; j < len(line); j++ {
					if line[j] == ';' {
						sepIdx = j
						break
					}
				}

				if sepIdx > 0 {
					station := UnsafeString(line[:sepIdx])
					measurement, err := ParseFloat(line[sepIdx+1:])
					if err == nil {
						stats, found := result.Get(station)
						if found {
							// Update existing stats
							stats.Min = minFloat(stats.Min, measurement)
							stats.Max = maxFloat(stats.Max, measurement)
							stats.Sum += measurement
							stats.Count++
						} else {
							// Create new stats
							result.Put(station, StationStats{
								Min:   measurement,
								Max:   measurement,
								Sum:   measurement,
								Count: 1,
							})
						}
					}
				}
			}
			start = i + 1
		}
	}
}

// Worker processes batches of data in parallel
// Reads from the jobs channel and sends results to the results channel
func Worker(id int, jobs <-chan []byte, results chan<- *StationMap, wg *sync.WaitGroup) {
	defer wg.Done()

	// Create a local map for this worker
	result := NewStationMap(ExpectedStations)

	// Process each batch
	for batch := range jobs {
		LineProcessor(batch, result)
	}

	// Send the results back
	results <- result
}

// Helper functions for min/max operations
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// minFloat returns the minimum of two floats
func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// maxFloat returns the maximum of two floats
func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// processFile processes the input file and returns statistics for each station
// Uses memory mapping and parallel processing for optimal performance
func processFile(filename string) error {
	startTime := time.Now()

	// Open the file with memory mapping for fast access
	reader, err := mmap.Open(filename)
	if err != nil {
		return err
	}
	defer reader.Close()

	fileSize := reader.Len()
	fmt.Printf("File opened with mmap in: %v\n", time.Since(startTime))
	fmt.Printf("File size: %.2f MB\n", float64(fileSize)/(1024*1024))

	// Determine the number of batches and workers
	numCPU := runtime.NumCPU()
	fmt.Printf("Using %d CPU cores\n", numCPU)

	batchCount := (fileSize + BatchSize - 1) / BatchSize
	fmt.Printf("Processing file in %d batches of ~%d MB each\n", batchCount, BatchSize/(1024*1024))

	// Create channels for job distribution and result collection
	jobs := make(chan []byte, batchCount)
	results := make(chan *StationMap, batchCount)

	// Start worker goroutines
	var workerWg sync.WaitGroup
	workerStart := time.Now()

	for i := 0; i < numCPU; i++ {
		workerWg.Add(1)
		go Worker(i, jobs, results, &workerWg)
	}

	fmt.Printf("Workers initialized in: %v\n", time.Since(workerStart))

	// Read the file in batches and send to workers
	readStart := time.Now()
	var batchesProcessed int64

	// Process the file in batches
	for offset := 0; offset < fileSize; {
		// Determine batch size
		end := offset + BatchSize
		if end > fileSize {
			end = fileSize
		}

		// Read the batch
		batch := make([]byte, end-offset)
		n, err := reader.ReadAt(batch, int64(offset))
		if err != nil && err != io.EOF {
			return err
		}

		// Find the last newline to ensure complete lines
		lastNewline := n - 1
		for lastNewline >= 0 && batch[lastNewline] != '\n' {
			lastNewline--
		}

		if lastNewline < 0 {
			// No newline found, process the entire batch
			lastNewline = n - 1
		}

		// Send the batch to a worker
		jobs <- batch[:lastNewline+1]
		atomic.AddInt64(&batchesProcessed, 1)

		// Move to the next batch
		offset += lastNewline + 1
	}

	// Close the jobs channel to signal workers to finish
	close(jobs)

	// Wait for all workers to finish
	workerWg.Wait()
	close(results)

	fmt.Printf("Reading complete: %d batches in %v\n", batchesProcessed, time.Since(readStart))

	// Merge results from all workers
	mergeStart := time.Now()
	finalResult := NewStationMap(ExpectedStations)
	resultCount := 0

	for workerResult := range results {
		resultCount++
		workerResult.Range(func(station string, stats *StationStats) bool {
			existingStats, found := finalResult.Get(station)
			if found {
				// Merge with existing stats
				existingStats.Min = minFloat(existingStats.Min, stats.Min)
				existingStats.Max = maxFloat(existingStats.Max, stats.Max)
				existingStats.Sum += stats.Sum
				existingStats.Count += stats.Count
			} else {
				// Add new station
				finalResult.Put(station, *stats)
			}
			return true
		})
	}

	fmt.Printf("All %d result batches processed in %v\n", resultCount, time.Since(mergeStart))

	// Format and print the results
	formatStart := time.Now()

	// Collect all stations
	var stations []string
	finalResult.Range(func(station string, stats *StationStats) bool {
		stations = append(stations, station)
		return true
	})

	// Sort stations alphabetically
	sort.Strings(stations)

	// Build the output string
	var sb strings.Builder
	sb.WriteString("{")

	for i, station := range stations {
		stats, _ := finalResult.Get(station)
		mean := stats.Sum / float64(stats.Count)

		sb.WriteString(station)
		sb.WriteString("=")
		sb.WriteString(fmt.Sprintf("%.1f/%.1f/%.1f", stats.Min, mean, stats.Max))

		if i < len(stations)-1 {
			sb.WriteString(", ")
		}
	}

	sb.WriteString("}")
	fmt.Printf("Output formatted for %d stations in: %v\n", len(stations), time.Since(formatStart))

	// Print the result
	fmt.Println(sb.String())

	return nil
}

func main() {
	startTime := time.Now()

	// Process the file
	err := processFile("/Users/thomasmcgeehan/billion/billion/data/data/measurements.txt")
	if err != nil {
		log.Fatal(err)
	}

	// Print total processing time
	fmt.Printf("Total processing time: %v\n", time.Since(startTime))
	fmt.Printf("Challenge completed in: %v\n", time.Since(startTime))
}
