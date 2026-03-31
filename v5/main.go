package main

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
	"syscall"
	"time"
)

const (
	hashSize = 1 << 17
	mask     = hashSize - 1
)

type entry struct {
	hash  uint64
	name  []byte // store raw bytes (no string in hot path)
	min   float64
	max   float64
	sum   float64
	count int64
	used  bool
}

type table struct {
	entries []entry
}

func newTable() *table {
	return &table{
		entries: make([]entry, hashSize),
	}
}

// FNV-1a
func hashBytes(b []byte) uint64 {
	var h uint64 = 14695981039346656037
	for _, c := range b {
		h ^= uint64(c)
		h *= 1099511628211
	}
	return h
}

// fast float parser
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

func (t *table) insert(name []byte, val float64) {
	h := hashBytes(name)
	idx := int(h) & mask

	for {
		e := &t.entries[idx]

		if !e.used {
			e.used = true
			e.hash = h

			// copy once per unique station
			nameCopy := make([]byte, len(name))
			copy(nameCopy, name)
			e.name = nameCopy

			e.min = val
			e.max = val
			e.sum = val
			e.count = 1
			return
		}

		if e.hash == h && len(e.name) == len(name) && bytes.Equal(e.name, name) {
			if val < e.min {
				e.min = val
			}
			if val > e.max {
				e.max = val
			}
			e.sum += val
			e.count++
			return
		}

		idx = (idx + 1) & mask
	}
}

func processChunk(data []byte, start, end int, out chan *table) {
	t := newTable()

	i := start
	for i < end {
		lineStart := i

		for i < end && data[i] != '\n' {
			i++
		}

		line := data[lineStart:i]
		i++

		if len(line) == 0 {
			continue
		}

		sep := bytes.IndexByte(line, ';')
		if sep < 0 {
			continue
		}

		name := line[:sep]
		valBytes := line[sep+1:]

		if len(valBytes) == 0 {
			continue
		}

		// trim CR if present
		if valBytes[len(valBytes)-1] == '\r' {
			valBytes = valBytes[:len(valBytes)-1]
			if len(valBytes) == 0 {
				continue
			}
		}

		val := parseFloat(valBytes)
		t.insert(name, val)
	}

	out <- t
}

func merge(dst, src *table) {
	for i := range src.entries {
		e := &src.entries[i]
		if !e.used {
			continue
		}

		idx := int(e.hash) & mask

		for {
			d := &dst.entries[idx]

			if !d.used {
				*d = *e
				return
			}

			if d.hash == e.hash && len(d.name) == len(e.name) && bytes.Equal(d.name, e.name) {
				if e.min < d.min {
					d.min = e.min
				}
				if e.max > d.max {
					d.max = e.max
				}
				d.sum += e.sum
				d.count += e.count
				break
			}

			idx = (idx + 1) & mask
		}
	}
}

func mmapFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return nil, err
	}

	return syscall.Mmap(int(f.Fd()), 0, int(st.Size()),
		syscall.PROT_READ, syscall.MAP_SHARED)
}

func main() {
	start := time.Now()

	data, err := mmapFile("/Users/tfmv/billion/billion/data/data/measurements.txt")
	if err != nil {
		panic(err)
	}

	numCPU := runtime.NumCPU()
	chunk := len(data) / numCPU

	out := make(chan *table, numCPU)
	var wg sync.WaitGroup

	for i := 0; i < numCPU; i++ {
		s := i * chunk
		e := s + chunk
		if i == numCPU-1 {
			e = len(data)
		}

		// align to newline
		if s != 0 {
			for s < len(data) && data[s-1] != '\n' {
				s++
			}
		}

		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			processChunk(data, start, end, out)
		}(s, e)
	}

	wg.Wait()
	close(out)

	final := newTable()
	for t := range out {
		merge(final, t)
	}

	type result struct {
		name string
		min  float64
		mean float64
		max  float64
	}

	var results []result
	for _, e := range final.entries {
		if !e.used {
			continue
		}
		results = append(results, result{
			name: string(e.name),
			min:  e.min,
			mean: e.sum / float64(e.count),
			max:  e.max,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].name < results[j].name
	})

	fmt.Print("{")
	for i, r := range results {
		if i > 0 {
			fmt.Print(", ")
		}
		fmt.Printf("%s=%.1f/%.1f/%.1f", r.name, r.min, r.mean, r.max)
	}
	fmt.Println("}")

	fmt.Printf("\nTime: %v\n", time.Since(start))
}
