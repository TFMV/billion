package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"time"
)

const (
	DataFile          = "/Users/thomasmcgeehan/billion/billion/data/data/measurements.txt"
	DefaultIterations = 5
)

type BenchmarkResult struct {
	Version     string
	Iterations  []time.Duration
	AverageTime time.Duration
	MedianTime  time.Duration
	MinTime     time.Duration
	MaxTime     time.Duration
	MemoryUsed  float64 // MB
	Throughput  float64 // million rows/second
}

func (r *BenchmarkResult) String() string {
	return fmt.Sprintf(
		"Version: %s\n"+
			"  Average Time: %v\n"+
			"  Median Time:  %v\n"+
			"  Min Time:     %v\n"+
			"  Max Time:     %v\n"+
			"  Memory Used:  %.2f MB\n"+
			"  Throughput:   %.2f million rows/second\n",
		r.Version,
		r.AverageTime,
		r.MedianTime,
		r.MinTime,
		r.MaxTime,
		r.MemoryUsed,
		r.Throughput,
	)
}

// Calculate median of durations
func median(durations []time.Duration) time.Duration {
	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	n := len(sorted)
	if n%2 == 0 {
		return (sorted[n/2-1] + sorted[n/2]) / 2
	}
	return sorted[n/2]
}

// Run a specific version
func runVersion(version string, cmdArgs []string) (time.Duration, float64) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	startAlloc := memStats.TotalAlloc

	start := time.Now()
	cmd := exec.Command(cmdArgs[0], cmdArgs[1:]...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		fmt.Printf("Error running %s: %v\n", version, err)
		fmt.Printf("Stderr: %s\n", stderr.String())
	}
	duration := time.Since(start)

	runtime.ReadMemStats(&memStats)
	memUsed := float64(memStats.TotalAlloc-startAlloc) / (1024 * 1024)

	return duration, memUsed
}

// Run benchmark for a specific version
func runBenchmark(version string, cmdArgs []string, iterations int) *BenchmarkResult {
	result := &BenchmarkResult{
		Version:    version,
		Iterations: make([]time.Duration, iterations),
	}

	var totalMemory float64

	fmt.Printf("Running %s benchmark (%d iterations)...\n", version, iterations)
	for i := 0; i < iterations; i++ {
		fmt.Printf("  Iteration %d/%d...\n", i+1, iterations)
		runtime.GC()                // Force garbage collection before each run
		time.Sleep(1 * time.Second) // Give system time to stabilize

		duration, memUsed := runVersion(version, cmdArgs)
		result.Iterations[i] = duration
		totalMemory += memUsed

		fmt.Printf("    Time: %v, Memory: %.2f MB\n", duration, memUsed)
	}

	// Calculate statistics
	var totalTime time.Duration
	minTime := result.Iterations[0]
	maxTime := result.Iterations[0]

	for _, t := range result.Iterations {
		totalTime += t
		if t < minTime {
			minTime = t
		}
		if t > maxTime {
			maxTime = t
		}
	}

	result.AverageTime = totalTime / time.Duration(iterations)
	result.MedianTime = median(result.Iterations)
	result.MinTime = minTime
	result.MaxTime = maxTime
	result.MemoryUsed = totalMemory / float64(iterations)
	result.Throughput = 1000.0 / result.AverageTime.Seconds() // Assuming 1 billion rows

	return result
}

func runBenchmarks() {
	// Parse command line arguments
	iterations := DefaultIterations
	if len(os.Args) > 1 {
		n, err := strconv.Atoi(os.Args[1])
		if err == nil && n > 0 {
			iterations = n
		} else {
			fmt.Printf("Invalid iteration count: %s. Using default: %d\n", os.Args[1], DefaultIterations)
		}
	}

	// Check if data file exists
	if _, err := os.Stat(DataFile); os.IsNotExist(err) {
		fmt.Printf("Data file not found: %s\n", DataFile)
		os.Exit(1)
	}

	fmt.Printf("Running benchmarks with %d iterations each\n", iterations)
	fmt.Printf("Data file: %s\n", DataFile)
	fmt.Printf("CPU cores: %d\n\n", runtime.NumCPU())

	// Run benchmarks
	v1Result := runBenchmark("V1", []string{"go", "run", "./v1/main.go"}, iterations)
	v2Result := runBenchmark("V2", []string{"go", "run", "./v2/main.go"}, iterations)
	v3Result := runBenchmark("V3", []string{"go", "run", "./v3/main.go"}, iterations)

	// Print results
	fmt.Println("\n=== BENCHMARK RESULTS ===")
	fmt.Println(v1Result)
	fmt.Println(v2Result)
	fmt.Println(v3Result)

	// Determine the fastest version
	fastest := v1Result
	if v2Result.AverageTime < fastest.AverageTime {
		fastest = v2Result
	}
	if v3Result.AverageTime < fastest.AverageTime {
		fastest = v3Result
	}

	fmt.Printf("\nFastest implementation: %s (%.2f million rows/second)\n",
		fastest.Version, fastest.Throughput)

	// Calculate speedups
	fmt.Printf("\nSpeedup Comparisons:\n")
	fmt.Printf("  V1 → V2: %.2fx\n", float64(v1Result.AverageTime)/float64(v2Result.AverageTime))
	fmt.Printf("  V1 → V3: %.2fx\n", float64(v1Result.AverageTime)/float64(v3Result.AverageTime))
	fmt.Printf("  V2 → V3: %.2fx\n", float64(v2Result.AverageTime)/float64(v3Result.AverageTime))

	// Save results to file
	saveResults(v1Result, v2Result, v3Result)
}

// Save results to a file
func saveResults(v1, v2, v3 *BenchmarkResult) {
	file, err := os.Create("benchmark_results.txt")
	if err != nil {
		fmt.Printf("Error creating results file: %v\n", err)
		return
	}
	defer file.Close()

	fmt.Fprintf(file, "=== BENCHMARK RESULTS ===\n")
	fmt.Fprintf(file, "Date: %s\n", time.Now().Format(time.RFC1123))
	fmt.Fprintf(file, "CPU: %d cores\n", runtime.NumCPU())
	fmt.Fprintf(file, "OS: %s\n\n", runtime.GOOS)

	fmt.Fprintf(file, "%s\n", v1.String())
	fmt.Fprintf(file, "%s\n", v2.String())
	fmt.Fprintf(file, "%s\n", v3.String())

	fastest := v1
	if v2.AverageTime < fastest.AverageTime {
		fastest = v2
	}
	if v3.AverageTime < fastest.AverageTime {
		fastest = v3
	}

	fmt.Fprintf(file, "\nFastest implementation: %s (%.2f million rows/second)\n",
		fastest.Version, fastest.Throughput)

	fmt.Fprintf(file, "\nSpeedup Comparisons:\n")
	fmt.Fprintf(file, "  V1 → V2: %.2fx\n", float64(v1.AverageTime)/float64(v2.AverageTime))
	fmt.Fprintf(file, "  V1 → V3: %.2fx\n", float64(v1.AverageTime)/float64(v3.AverageTime))
	fmt.Fprintf(file, "  V2 → V3: %.2fx\n", float64(v2.AverageTime)/float64(v3.AverageTime))

	fmt.Printf("Results saved to benchmark_results.txt\n")
}

func main() {
	runBenchmarks()
}
