package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"
)

func main() {
	// Parse command line arguments
	numRecords := flag.Int64("n", 1000000, "Number of records to generate (default: 1 million)")
	outputFile := flag.String("o", "data/measurements.txt", "Output file path")
	flag.Parse()

	fmt.Printf("Generating %d records to %s\n", *numRecords, *outputFile)

	// Create output directory if it doesn't exist
	os.MkdirAll("data", 0755)

	// Create output file
	file, err := os.Create(*outputFile)
	if err != nil {
		fmt.Printf("Error creating file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	// Use buffered writer for better performance
	writer := bufio.NewWriter(file)
	defer writer.Flush()

	// Initialize random number generator
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Generate station names (s1 through s100)
	stationCount := 100
	stations := make([]string, stationCount)
	for i := 0; i < stationCount; i++ {
		stations[i] = fmt.Sprintf("s%d", i+1)
	}

	// Generate records
	for i := int64(0); i < *numRecords; i++ {
		// Pick a random station
		station := stations[r.Intn(stationCount)]

		// Generate a random temperature between -50.0 and 50.0
		temp := r.Float64()*100.0 - 50.0

		// Write the record
		fmt.Fprintf(writer, "%s;%.1f\n", station, temp)

		// Flush every 10 million records to avoid using too much memory
		if i > 0 && i%10000000 == 0 {
			writer.Flush()
			fmt.Printf("Generated %d records (%.1f%%)\n", i, float64(i)*100.0/float64(*numRecords))
		}
	}

	fmt.Println("Data generation complete!")
}
