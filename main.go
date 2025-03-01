package main

import (
	"bytes"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/mmap"
)

type Stats struct {
	Min   float64
	Max   float64
	Sum   float64
	Count int64
}

func worker(id int, jobs <-chan []byte, results chan<- map[string]*Stats, wg *sync.WaitGroup) {
	defer wg.Done()
	// Process each batch of lines
	for batch := range jobs {
		statsMap := make(map[string]*Stats, 100) // Pre-allocate with larger capacity

		// Split the batch into lines
		lines := bytes.Split(batch, []byte("\n"))

		for _, line := range lines {
			if len(line) == 0 {
				continue // Skip empty lines
			}

			// Find the separator position
			sepIdx := bytes.IndexByte(line, ';')
			if sepIdx < 0 {
				continue // Skip invalid lines
			}

			// Extract station name and measurement
			station := string(line[:sepIdx])
			measurement, err := strconv.ParseFloat(string(line[sepIdx+1:]), 64)
			if err != nil {
				continue
			}

			stat, exists := statsMap[station]
			if !exists {
				statsMap[station] = &Stats{
					Min:   measurement,
					Max:   measurement,
					Sum:   measurement,
					Count: 1,
				}
			} else {
				if measurement < stat.Min {
					stat.Min = measurement
				}
				if measurement > stat.Max {
					stat.Max = measurement
				}
				stat.Sum += measurement
				stat.Count++
			}
		}
		// Send results
		results <- statsMap
	}
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
	batchSize := int64(4 * 1024 * 1024) // Default: 4MB per batch
	if fileSize > 1024*1024*1024 {      // If file is larger than 1GB
		batchSize = 8 * 1024 * 1024 // Use 8MB per batch
	}

	// Calculate number of batches
	numBatches := (fileSize + batchSize - 1) / batchSize // Ceiling division
	fmt.Printf("Processing file in %d batches of ~%d MB each\n",
		numBatches, batchSize/(1024*1024))

	// Create channels with sufficient buffer to avoid deadlocks
	jobBuffer := numCPU * 4 // Buffer size for job channel
	jobs := make(chan []byte, jobBuffer)

	// Create a separate channel for results with a large buffer
	resultBuffer := numCPU * 8 // Buffer size for result channel
	results := make(chan map[string]*Stats, resultBuffer)

	// Start worker timing
	startWorkers := time.Now()

	// Create a WaitGroup to track worker completion
	var workerWg sync.WaitGroup

	// Start workers
	for i := 0; i < numCPU; i++ {
		workerWg.Add(1)
		go worker(i, jobs, results, &workerWg)
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

		}

		fmt.Printf("Reading complete: %d batches in %v\n",
			batchCount, time.Since(startReading))
	}()

	// Start aggregation timing
	startAggregation := time.Now()

	// Use sync.Map for lock-free concurrent access
	var finalStats sync.Map
	resultCount := 0

	// Process results as they come in
	for partialStats := range results {
		resultCount++

		// Process each station in the partial results
		for station, stat := range partialStats {
			// Try to load existing stats or store new ones atomically
			existingVal, loaded := finalStats.LoadOrStore(station, &Stats{
				Min:   stat.Min,
				Max:   stat.Max,
				Sum:   stat.Sum,
				Count: stat.Count,
			})

			// If we found existing stats, update them atomically
			if loaded {
				existingStat := existingVal.(*Stats)

				// Use atomic operations for updating the stats
				// This is a critical section, but sync.Map helps reduce contention
				func() {
					// Create a very small critical section with a mutex
					// This is much more efficient than locking the entire map
					mu := &sync.Mutex{}
					mu.Lock()
					defer mu.Unlock()

					// Update the stats
					if stat.Min < existingStat.Min {
						existingStat.Min = stat.Min
					}
					if stat.Max > existingStat.Max {
						existingStat.Max = stat.Max
					}
					existingStat.Sum += stat.Sum
					existingStat.Count += stat.Count
				}()
			}
		}

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

	// Convert sync.Map to regular map for output formatting
	stationCount := 0
	finalStats.Range(func(key, value interface{}) bool {
		station := key.(string)
		stat := value.(*Stats)
		stationCount++

		if !first {
			sb.WriteString(", ")
		}
		mean := stat.Sum / float64(stat.Count)
		sb.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f", station, stat.Min, mean, stat.Max))
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
