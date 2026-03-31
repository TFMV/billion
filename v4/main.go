package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"time"
)

type stats struct {
	min, max, sum float64
	count         int64
	name          string
}

type part struct {
	offset, size int64
}

const fnvOffset64 = 14695981039346656037
const fnvPrime64 = 1099511628211

func hashBytes(b []byte) uint64 {
	h := uint64(fnvOffset64)
	for _, c := range b {
		h ^= uint64(c)
		h *= fnvPrime64
	}
	return h
}

// fast ASCII float parser (no strconv)
func parseFloat(b []byte) float64 {
	if len(b) == 0 {
		return 0
	}

	var sign float64 = 1
	i := 0

	if b[0] == '-' {
		sign = -1
		i++
	}

	var intPart float64
	for ; i < len(b) && b[i] != '.'; i++ {
		intPart = intPart*10 + float64(b[i]-'0')
	}

	if i == len(b) {
		return sign * intPart
	}

	i++ // skip '.'

	var frac float64
	div := 1.0
	for ; i < len(b); i++ {
		frac = frac*10 + float64(b[i]-'0')
		div *= 10
	}

	return sign * (intPart + frac/div)
}

func r8Process(input []byte, start, end int64, out chan map[uint64]*stats) {
	buf := input[start:end]

	m := make(map[uint64]*stats, 1024)

	var i int
	for i < len(buf) {
		lineStart := i

		for i < len(buf) && buf[i] != '\n' {
			i++
		}

		line := buf[lineStart:i]
		i++

		sep := bytes.IndexByte(line, ';')
		if sep < 0 {
			continue
		}

		keyBytes := line[:sep]
		valBytes := line[sep+1:]

		h := hashBytes(keyBytes)

		s, ok := m[h]
		if !ok {
			// only allocation per unique station
			name := string(keyBytes)

			f := parseFloat(valBytes)
			m[h] = &stats{
				min:   f,
				max:   f,
				sum:   f,
				count: 1,
				name:  name,
			}
			continue
		}

		f := parseFloat(valBytes)

		if f < s.min {
			s.min = f
		}
		if f > s.max {
			s.max = f
		}
		s.sum += f
		s.count++
	}

	out <- m
}

func r8(inputPath string, w io.Writer) error {
	data, err := os.ReadFile(inputPath)
	if err != nil {
		return err
	}

	num := runtime.NumCPU()

	// naive split (assumes pre-split safe or pre-aligned dataset)
	chunkSize := len(data) / num

	ch := make(chan map[uint64]*stats, num)

	for i := 0; i < num; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if i == num-1 {
			end = len(data)
		}
		go r8Process(data, int64(start), int64(end), ch)
	}

	global := make(map[uint64]*stats, 1024)

	for i := 0; i < num; i++ {
		part := <-ch
		for k, v := range part {
			g, ok := global[k]
			if !ok {
				global[k] = v
				continue
			}

			if v.min < g.min {
				g.min = v.min
			}
			if v.max > g.max {
				g.max = v.max
			}
			g.sum += v.sum
			g.count += v.count
		}
	}

	names := make([]string, 0, len(global))
	for _, v := range global {
		names = append(names, v.name)
	}
	sort.Strings(names)

	fmt.Fprint(w, "{")
	for i, name := range names {
		for _, v := range global {
			if v.name == name {
				mean := v.sum / float64(v.count)
				if i > 0 {
					fmt.Fprint(w, ", ")
				}
				fmt.Fprintf(w, "%s=%.1f/%.1f/%.1f",
					name, v.min, mean, v.max)
				break
			}
		}
	}
	fmt.Fprint(w, "}\n")

	return nil
}

func main() {
	start := time.Now()

	r8("/Users/tfmv/billion/billion/data/data/measurements.txt", os.Stdout)

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	fmt.Printf("\n=== Benchmark ===\n")
	fmt.Printf("Time: %v\n", time.Since(start))
	fmt.Printf("Alloc: %.2f MB\n", float64(m.TotalAlloc)/1024/1024)
}
