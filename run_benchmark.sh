#!/bin/bash

# Set the number of iterations (can be overridden with command line argument)
ITERATIONS=${1:-5}

# Update the number of iterations in the benchmark file
sed -i '' "s/DefaultIterations = [0-9]*/DefaultIterations = $ITERATIONS/" run_benchmarks.go

echo "Running benchmark with $ITERATIONS iterations per implementation..."
echo "This may take some time. Please be patient."
echo ""

# Run the benchmark program directly
go run run_benchmarks.go

echo ""
echo "Benchmark complete!" 