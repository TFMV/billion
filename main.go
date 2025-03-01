package main

import (
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"golang.org/x/exp/mmap"
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

// StationMap is a custom hash table optimized for station names
// It uses open addressing with linear probing
type StationMap struct {
	keys     []string
	values   []StationStats
	occupied []bool
	size     int
	capacity int
	mask     int
}

// NewStationMap creates a new StationMap with the given capacity
func NewStationMap(capacity int) *StationMap {
	// Round up to power of 2 for fast modulo with mask
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

// hash returns a hash of the string s
func hash(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// Get returns the stats for the given station
func (m *StationMap) Get(station string) (*StationStats, bool) {
	h := hash(station) & uint32(m.mask)
	for {
		if !m.occupied[h] {
			return nil, false
		}
		if m.keys[h] == station {
			return &m.values[h], true
		}
		h = (h + 1) & uint32(m.mask)
	}
}

// Put adds or updates the stats for the given station
func (m *StationMap) Put(station string, stats StationStats) {
	// Grow if load factor exceeds 0.7
	if float64(m.size+1)/float64(m.capacity) > 0.7 {
		m.grow()
	}

	h := hash(station) & uint32(m.mask)
	for {
		if !m.occupied[h] {
			m.keys[h] = station
			m.values[h] = stats
			m.occupied[h] = true
			m.size++
			return
		}
		if m.keys[h] == station {
			m.values[h] = stats
			return
		}
		h = (h + 1) & uint32(m.mask)
	}
}

// grow increases the capacity of the map
func (m *StationMap) grow() {
	newCapacity := m.capacity * 2
	newMap := NewStationMap(newCapacity)

	for i := 0; i < m.capacity; i++ {
		if m.occupied[i] {
			newMap.Put(m.keys[i], m.values[i])
		}
	}

	m.keys = newMap.keys
	m.values = newMap.values
	m.occupied = newMap.occupied
	m.size = newMap.size
	m.capacity = newMap.capacity
	m.mask = newMap.mask
}

// Range calls f for each station in the map
func (m *StationMap) Range(f func(station string, stats *StationStats) bool) {
	for i := 0; i < m.capacity; i++ {
		if m.occupied[i] {
			if !f(m.keys[i], &m.values[i]) {
				return
			}
		}
	}
}

// ByteSlice is a slice of bytes
type ByteSlice []byte

// ParseFloat parses a float from a byte slice without allocations
func ParseFloat(b ByteSlice) (float64, error) {
	// Fast path for common case
	if len(b) == 0 {
		return 0, fmt.Errorf("empty string")
	}

	// Handle negative numbers
	neg := false
	if b[0] == '-' {
		neg = true
		b = b[1:]
	}

	// Parse integer part
	var val float64
	var i int
	for i = 0; i < len(b) && b[i] >= '0' && b[i] <= '9'; i++ {
		val = val*10 + float64(b[i]-'0')
	}

	// Parse decimal part
	if i < len(b) && b[i] == '.' {
		pow10 := 1.0
		for j := i + 1; j < len(b); j++ {
			if b[j] < '0' || b[j] > '9' {
				return 0, fmt.Errorf("invalid character in decimal part")
			}
			pow10 *= 10.0
			val += float64(b[j]-'0') / pow10
		}
	} else if i < len(b) {
		return 0, fmt.Errorf("invalid character in number")
	}

	if neg {
		val = -val
	}
	return val, nil
}

// UnsafeString converts a byte slice to a string without allocation
func UnsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// LineProcessor processes a batch of lines
func LineProcessor(batch []byte, result *StationMap) {
	// Split the batch into lines
	var start int
	for i := 0; i < len(batch); i++ {
		if batch[i] == '\n' {
			line := batch[start:i]
			if len(line) > 0 {
				// Find the separator position
				sepIdx := -1
				for j := 0; j < len(line); j++ {
					if line[j] == ';' {
						sepIdx = j
						break
					}
				}

				if sepIdx >= 0 {
					// Extract station name and measurement
					station := UnsafeString(line[:sepIdx])
					measurement, err := ParseFloat(line[sepIdx+1:])
					if err == nil {
						// Update stats
						stats, exists := result.Get(station)
						if !exists {
							result.Put(station, StationStats{
								Min:   measurement,
								Max:   measurement,
								Sum:   measurement,
								Count: 1,
							})
						} else {
							if measurement < stats.Min {
								stats.Min = measurement
							}
							if measurement > stats.Max {
								stats.Max = measurement
							}
							stats.Sum += measurement
							stats.Count++
						}
					}
				}
			}
			start = i + 1
		}
	}
}

// Worker processes batches of data
func Worker(id int, jobs <-chan []byte, results chan<- *StationMap, wg *sync.WaitGroup) {
	defer wg.Done()

	// Pre-allocate a result map for this worker
	result := NewStationMap(200) // Expect around 200 stations

	// Process each batch
	for batch := range jobs {
		LineProcessor(batch, result)
	}

	// Send the result
	results <- result
}

// min returns the smaller of two int64 values
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// minFloat returns the smaller of two float64 values
func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// maxFloat returns the larger of two float64 values
func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func processFile(filename string) error {
	// Start total timing
	startTotal := time.Now()

	// Open the file using memory mapping
	reader, err := mmap.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to mmap file: %v", err)
	}
	defer reader.Close()

	fmt.Printf("File opened with mmap in: %v\n", time.Since(startTotal))

	// Get file size for dynamic buffer sizing
	fileSize := int64(reader.Len())
	fmt.Printf("File size: %.2f MB\n", float64(fileSize)/(1024*1024))

	// Determine optimal number of workers
	numCPU := runtime.NumCPU()
	fmt.Printf("Using %d CPU cores\n", numCPU)

	// Dynamically adjust batch size based on file size
	// For very large files, use larger batches
	batchSize := int64(8 * 1024 * 1024) // Default: 8MB per batch
	if fileSize > 5*1024*1024*1024 {    // If file is larger than 5GB
		batchSize = 16 * 1024 * 1024 // Use 16MB per batch
	}

	// Calculate number of batches
	numBatches := (fileSize + batchSize - 1) / batchSize // Ceiling division
	fmt.Printf("Processing file in %d batches of ~%d MB each\n",
		numBatches, batchSize/(1024*1024))

	// Create channels with sufficient buffer to avoid deadlocks
	jobBuffer := numCPU * 2 // Buffer size for job channel
	jobs := make(chan []byte, jobBuffer)

	// Create a separate channel for results with a buffer
	resultBuffer := numCPU // Buffer size for result channel
	results := make(chan *StationMap, resultBuffer)

	// Start worker timing
	startWorkers := time.Now()

	// Create a WaitGroup to track worker completion
	var workerWg sync.WaitGroup

	// Start workers
	for i := 0; i < numCPU; i++ {
		workerWg.Add(1)
		go Worker(i, jobs, results, &workerWg)
	}

	// Create a goroutine to close the results channel when all workers are done
	go func() {
		workerWg.Wait()
		close(results)
	}()

	fmt.Printf("Workers initialized in: %v\n", time.Since(startWorkers))

	// Start reading timing
	startReading := time.Now()

	// Create a goroutine to read the file and send batches to workers
	var fileReadWg sync.WaitGroup
	fileReadWg.Add(1)

	// Track progress
	var bytesProcessed int64

	go func() {
		defer fileReadWg.Done()
		defer close(jobs) // Close jobs channel when done reading

		// Process the file in batches
		batchCount := 0

		for offset := int64(0); offset < fileSize; offset += batchSize {
			// Calculate the size of this batch (might be smaller for the last batch)
			size := min(batchSize, fileSize-offset)

			// Create a buffer for this batch
			batch := make([]byte, size)

			// Read the batch from the memory-mapped file
			n, err := reader.ReadAt(batch, offset)
			if err != nil || int64(n) != size {
				fmt.Printf("Error reading batch at offset %d: %v\n", offset, err)
				continue
			}

			// Send the batch to a worker
			jobs <- batch
			batchCount++

			// Update progress
			atomic.AddInt64(&bytesProcessed, size)

		}

		fmt.Printf("Reading complete: %d batches in %v\n",
			batchCount, time.Since(startReading))
	}()

	// Start aggregation timing
	startAggregation := time.Now()

	// Create a final map to store the results
	finalMap := NewStationMap(200) // Expect around 200 stations
	resultCount := 0

	// Process results as they come in
	for workerMap := range results {
		resultCount++

		// Merge the worker's map into the final map
		workerMap.Range(func(station string, stats *StationStats) bool {
			finalStats, exists := finalMap.Get(station)
			if !exists {
				finalMap.Put(station, *stats)
			} else {
				finalStats.Min = minFloat(finalStats.Min, stats.Min)
				finalStats.Max = maxFloat(finalStats.Max, stats.Max)
				finalStats.Sum += stats.Sum
				finalStats.Count += stats.Count
			}
			return true
		})
	}

	// Wait for file reading to complete (should already be done)
	fileReadWg.Wait()

	fmt.Printf("All %d result batches processed in %v\n", resultCount, time.Since(startAggregation))

	// Start output formatting timing
	startFormatting := time.Now()

	// Format the output
	var sb strings.Builder
	sb.WriteString("{")
	first := true

	// Convert map to output format
	stationCount := 0
	finalMap.Range(func(station string, stats *StationStats) bool {
		stationCount++

		if !first {
			sb.WriteString(", ")
		}
		mean := stats.Sum / float64(stats.Count)
		sb.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f", station, stats.Min, mean, stats.Max))
		first = false
		return true
	})

	sb.WriteString("}")
	fmt.Printf("Output formatted for %d stations in: %v\n", stationCount, time.Since(startFormatting))

	// Print final result
	fmt.Println(sb.String())

	// Print total time
	fmt.Printf("Total processing time: %v\n", time.Since(startTotal))

	return nil
}

func main() {
	startTime := time.Now()
	fmt.Printf("Starting billion row challenge at: %v\n", startTime.Format(time.RFC3339))

	if err := processFile("data/measurements.txt"); err != nil {
		log.Fatalf("Error processing file: %v", err)
	}

	fmt.Printf("Challenge completed in: %v\n", time.Since(startTime))
}
