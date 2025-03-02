package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)

const (
	MaxStations    = 10000             // Maximum number of unique stations.
	MaxTemperature = 999               // Max temperature (used for array size).  One decimal place.
	BatchSizeBytes = 256 * 1024 * 1024 // 256 MB batch size.
	MaxLineLength  = 256
)

type Station struct {
	Name  string
	Min   int16
	Max   int16
	Sum   int32
	Count int32
}

type stationName struct {
	hashVal uint32
	byteLen int
	name    [MaxLineLength]byte // Fixed-size buffer
}

type Processor struct {
	stationsData    []Station
	stationPointers []int
}

func (p *Processor) GetStationIdx(name []byte, nameLen int, hashVal uint32) int {
	idx := int(hashVal) & (len(p.stationPointers) - 1)

	for {
		currentPtr := p.stationPointers[idx]
		if currentPtr == -1 {
			return -1
		}

		existingStation := &p.stationsData[currentPtr]
		if existingStation.Count != 0 && string(existingStation.Name) == string(name[:nameLen]) {
			return currentPtr
		}

		idx = (idx + 1) & (len(p.stationPointers) - 1)
	}
}

func (p *Processor) CreateStation(name stationName, value int16) int {
	// Handle potential capacity overflow. Start with a reasonable size.
	if len(p.stationsData) >= MaxStations {
		panic("Too many stations!") // Handle this more gracefully if needed
	}
	idx := len(p.stationsData) // Get the next available index.
	p.stationsData = append(p.stationsData, Station{
		Name:  string(name.name[:name.byteLen]), // Store the name.
		Min:   value,
		Max:   value,
		Sum:   int32(value),
		Count: 1,
	})
	p.insertIntoTable(name)
	return idx
}

func (p *Processor) insertIntoTable(name stationName) {
	hash := name.hashVal
	tableSize := len(p.stationPointers)
	index := int(hash) & (tableSize - 1) // This is safer than a raw modulo.

	for {
		if p.stationPointers[index] == -1 { // Empty slot.
			p.stationPointers[index] = len(p.stationsData) - 1 // Store the index.
			break
		}
		index = (index + 1) & (tableSize - 1)
	}
}

func newProcessor() *Processor {
	size := 1
	for size < MaxStations {
		size *= 2
	}
	stationPointers := make([]int, size)
	for i := range stationPointers {
		stationPointers[i] = -1
	}
	return &Processor{
		stationsData:    make([]Station, 0, MaxStations),
		stationPointers: stationPointers,
	}
}

func parseMeasurement(bytes []byte) (int16, bool) {
	if len(bytes) < 3 { // Minimum length for a valid measurement (e.g., "1.2")
		return 0, false
	}

	var value int16
	i := 0
	negative := false

	if bytes[i] == '-' {
		negative = true
		i++
		if i >= len(bytes) {
			return 0, false
		}
	}

	// Need at least 3 more characters for a valid measurement
	if i+2 >= len(bytes) {
		return 0, false
	}

	// Check if we have X.Y or XX.Y format
	if bytes[i+1] == '.' {
		if i+2 >= len(bytes) { // Need one more digit after decimal
			return 0, false
		}
		// X.Y format
		if bytes[i] < '0' || bytes[i] > '9' || bytes[i+2] < '0' || bytes[i+2] > '9' {
			return 0, false
		}
		value = int16(bytes[i]-'0')*10 + int16(bytes[i+2]-'0')
	} else {
		if i+3 >= len(bytes) { // Need XX.Y format
			return 0, false
		}
		if bytes[i+2] != '.' ||
			bytes[i] < '0' || bytes[i] > '9' ||
			bytes[i+1] < '0' || bytes[i+1] > '9' ||
			bytes[i+3] < '0' || bytes[i+3] > '9' {
			return 0, false
		}
		value = int16(bytes[i]-'0')*100 + int16(bytes[i+1]-'0')*10 + int16(bytes[i+3]-'0')
	}

	if negative {
		value = -value
	}

	return value, true
}

func hashString(s []byte) uint32 {
	var h uint32 = 2166136261
	for _, b := range s {
		h ^= uint32(b)
		h *= 16777619
	}
	return h
}

// unsafeString converts a byte slice to a string without allocation
func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func (p *Processor) merge(other *Processor) {
	for i := 0; i < len(other.stationsData); i++ {
		otherStation := &other.stationsData[i]
		if otherStation.Count > 0 {
			nameBytes := []byte(otherStation.Name) // Efficient because p.stations is a slice.
			hashVal := hashString(nameBytes)

			idx := p.GetStationIdx(nameBytes, len(nameBytes), hashVal)

			if idx == -1 {
				newLength := len(otherStation.Name)
				var station stationName
				copy(station.name[:], otherStation.Name)
				station.hashVal = hashVal
				station.byteLen = newLength
				idx = p.CreateStation(station, otherStation.Min)
			}

			p.stationsData[idx].Min = minI16(p.stationsData[idx].Min, otherStation.Min)
			p.stationsData[idx].Max = maxI16(p.stationsData[idx].Max, otherStation.Max)
			p.stationsData[idx].Sum += otherStation.Sum
			p.stationsData[idx].Count += otherStation.Count
		}
	}
}

func minI16(a, b int16) int16 {
	if a < b {
		return a
	}
	return b
}

func maxI16(a, b int16) int16 {
	if a > b {
		return a
	}
	return b
}

func processChunk(chunk []byte, processor *Processor) {
	start := 0

	for i := 0; i < len(chunk); i++ {
		if chunk[i] == '\n' {
			line := chunk[start:i]
			if len(line) > 0 {
				// Split on ';'
				separatorIndex := -1
				for j := 0; j < len(line); j++ {
					if line[j] == ';' {
						separatorIndex = j
						break
					}
				}

				// Skip invalid lines without separator
				if separatorIndex <= 0 {
					start = i + 1
					continue
				}

				stationNameSlice := line[:separatorIndex]
				measurementBytes := line[separatorIndex+1:]

				hashVal := hashString(stationNameSlice)
				value, ok := parseMeasurement(measurementBytes)
				if ok {
					idx := processor.GetStationIdx(stationNameSlice, len(stationNameSlice), hashVal) // Try to find existing.
					if idx == -1 {                                                                   // Didn't find it.
						var station stationName
						copy(station.name[:], stationNameSlice)
						station.hashVal = hashVal
						station.byteLen = len(stationNameSlice)
						idx = processor.CreateStation(station, value)

					}
					stationData := &processor.stationsData[idx]
					if value < stationData.Min {
						stationData.Min = value
					}
					if value > stationData.Max {
						stationData.Max = value
					}
					stationData.Sum += int32(value) // Accumulate as int32
					stationData.Count++
				}
			}
			start = i + 1
		}
	}
}

func main() {
	start := time.Now()
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	startAlloc := memStats.TotalAlloc

	file, err := os.Open("/Users/thomasmcgeehan/billion/billion/data/data/measurements.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	numCPU := runtime.NumCPU()
	chunks := make(chan []byte, numCPU*2)
	results := make(chan *Processor, numCPU)
	var wg sync.WaitGroup

	// Start worker goroutines.
	for i := 0; i < numCPU; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			processor := newProcessor()
			for chunk := range chunks {
				processChunk(chunk, processor)
			}
			results <- processor
		}()
	}

	go func() {
		// Create a buffered reader for efficient file reading.
		reader := bufio.NewReaderSize(file, BatchSizeBytes)
		buf := make([]byte, BatchSizeBytes)
		for {
			bytesRead, err := reader.Read(buf)
			if bytesRead > 0 {
				// Trim to the actual data read and send to the channel.
				chunk := buf[:bytesRead]
				chunks <- chunk
			}

			if err != nil {
				break // EOF or an actual error.
			}
		}
		close(chunks) // No more chunks.
	}()

	wg.Wait()      // Wait for workers.
	close(results) // Close the results channel

	// Aggregate results.
	aggregated := newProcessor()
	for result := range results {
		aggregated.merge(result)
	}

	// Sort the stations by name.
	sort.Slice(aggregated.stationsData, func(i, j int) bool {
		return aggregated.stationsData[i].Name < aggregated.stationsData[j].Name
	})

	// Output the results.
	var outputBuilder strings.Builder
	outputBuilder.WriteString("{")
	for i, station := range aggregated.stationsData {
		if station.Count > 0 { // Ensure we only output valid entries.
			mean := float64(station.Sum) / float64(station.Count) / 10.0

			outputBuilder.WriteString(station.Name)
			outputBuilder.WriteString("=")
			outputBuilder.WriteString(strconv.FormatFloat(float64(station.Min)/10.0, 'f', 1, 64))
			outputBuilder.WriteString("/")
			outputBuilder.WriteString(strconv.FormatFloat(mean, 'f', 1, 64))
			outputBuilder.WriteString("/")
			outputBuilder.WriteString(strconv.FormatFloat(float64(station.Max)/10.0, 'f', 1, 64))

			if i < len(aggregated.stationsData)-1 {
				outputBuilder.WriteString(", ")
			}
		}
	}
	outputBuilder.WriteString("}")
	fmt.Println(outputBuilder.String())

	// Print benchmark summary
	duration := time.Since(start)
	runtime.ReadMemStats(&memStats)
	memUsed := memStats.TotalAlloc - startAlloc

	fmt.Printf("\n=== Benchmark Summary ===\n")
	fmt.Printf("Total Processing Time: %v\n", duration)
	fmt.Printf("Throughput: %.2f million rows/second\n", 1000.0/duration.Seconds())
	fmt.Printf("Memory Used: %.2f MB\n", float64(memUsed)/(1024*1024))
	fmt.Printf("Number of CPUs: %d\n", runtime.NumCPU())
	fmt.Printf("Batch Size: %d MB\n", BatchSizeBytes/(1024*1024))
	fmt.Printf("=====================\n")
}
