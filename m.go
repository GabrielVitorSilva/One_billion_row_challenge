package main

import (
	"bytes"
	"fmt"
	"log"
	"runtime"
	"sort"
	"sync"
	"time"

	"golang.org/x/exp/mmap"
)

type Stats struct {
	Min, Max, Sum int64
	Count         int64
}

func main() {
	start := time.Now()

	numWorkers := runtime.NumCPU()
	runtime.GOMAXPROCS(numWorkers)

	readerAt, err := mmap.Open("measurements.txt")
	if err != nil {
		log.Fatalf("Erro ao abrir o arquivo: %v", err)
	}
	defer readerAt.Close()

	fileSize := int64(readerAt.Len())
	chunkSize := fileSize / int64(numWorkers)

	resultsChan := make(chan map[string]Stats, numWorkers)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)

		start := int64(i) * chunkSize
		end := start + chunkSize
		if i == numWorkers-1 {
			end = fileSize
		}

		go func(start, end int64) {
			defer wg.Done()

			if start != 0 {
				start = findNextLineStart(readerAt, start)
			}
			if end != fileSize {
				end = findNextLineStart(readerAt, end)
			}

			if start >= end {
				resultsChan <- make(map[string]Stats)
				return
			}

			stationStats := processChunk(readerAt, start, end)
			resultsChan <- stationStats
		}(start, end)
	}

	wg.Wait()
	close(resultsChan)

	finalResults := make(map[string]Stats)
	for result := range resultsChan {
		for station, stats := range result {
			if s, ok := finalResults[station]; ok {
				s.Min = min(s.Min, stats.Min)
				s.Max = max(s.Max, stats.Max)
				s.Sum += stats.Sum
				s.Count += stats.Count
				finalResults[station] = s
			} else {
				finalResults[station] = stats
			}
		}
	}

	printResults(finalResults)

	fmt.Printf("Execution time: %s\n", time.Since(start))
}

func processChunk(readerAt *mmap.ReaderAt, start, end int64) map[string]Stats {
	chunkData := make([]byte, end-start)
	_, err := readerAt.ReadAt(chunkData, start)
	if err != nil {
		log.Fatalf("Erro ao ler o chunk: %v", err)
	}

	stationStats := make(map[string]Stats)
	data := chunkData

	for len(data) > 0 {
		semicolonPos := bytes.IndexByte(data, ';')
		if semicolonPos == -1 {
			break
		}

		newlinePos := bytes.IndexByte(data[semicolonPos:], '\n')
		if newlinePos == -1 {
			newlinePos = len(data[semicolonPos:])
		}

		lineEnd := semicolonPos + newlinePos
		station := string(data[:semicolonPos])
		tempStr := data[semicolonPos+1 : lineEnd]

		temp := parseTemp(tempStr)

		if s, ok := stationStats[station]; ok {
			s.Min = min(s.Min, temp)
			s.Max = max(s.Max, temp)
			s.Sum += temp
			s.Count++
			stationStats[station] = s
		} else {
			stationStats[station] = Stats{Min: temp, Max: temp, Sum: temp, Count: 1}
		}

		data = data[lineEnd+1:]
	}

	return stationStats
}

func findNextLineStart(readerAt *mmap.ReaderAt, offset int64) int64 {
	buf := make([]byte, 1)
	for offset < int64(readerAt.Len()) {
		if _, err := readerAt.ReadAt(buf, offset); err != nil {
			break
		}
		if buf[0] == '\n' {
			return offset + 1
		}
		offset++
	}
	return offset
}

func parseTemp(tempStr []byte) int64 {
	var temp int64
	var sign int64 = 1
	i := 0

	if tempStr[0] == '-' {
		sign = -1
		i++
	}

	for ; tempStr[i] != '.'; i++ {
		temp = temp*10 + int64(tempStr[i]-'0')
	}

	i++
	temp = temp*10 + int64(tempStr[i]-'0')

	return temp * sign
}

func printResults(results map[string]Stats) {
	stations := make([]string, 0, len(results))
	for station := range results {
		stations = append(stations, station)
	}
	sort.Strings(stations)

	fmt.Print("{")
	for i, station := range stations {
		if i > 0 {
			fmt.Print(", ")
		}
		s := results[station]
		mean := float64(s.Sum) / 10.0 / float64(s.Count)
		fmt.Printf(
			"%s=%.1f/%.1f/%.1f",
			station,
			float64(s.Min)/10.0,
			mean,
			float64(s.Max)/10.0,
		)
	}
	fmt.Println("}")
}
