package main

import (
	"compress/gzip"
	"container/heap"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
)

type jsonRow map[string]interface{}

func practiceIDMatches(row jsonRow, filter map[string]interface{}) bool {
	message := (row["message"]).(map[string]interface{})
	practiceID, _ := message["practice_id"]
	rowPracticeID := int(practiceID.(float64))
	filterPracticeID, filterPresent := filter["practice_id"]
	if filterPresent && filterPracticeID != rowPracticeID {
		return false
	}
	return true
}

func requestIDMatches(row jsonRow, filter map[string]interface{}) bool {
	message := (row["message"]).(map[string]interface{})
	requestID, _ := message["request_id"]
	filterRequestID, filterPresent := filter["request_id"]
	if filterPresent && filterRequestID != requestID {
		return false
	}
	return true
}

func rowMatchesFilters(row jsonRow, filter map[string]interface{}) bool {
	return practiceIDMatches(row, filter) && requestIDMatches(row, filter)
}

func filterJSON(
	row jsonRow,
	pattern *regexp.Regexp,
	sortChannel chan jsonRow,
	wg *sync.WaitGroup,
	filterValues map[string]interface{}) {

	if !rowMatchesFilters(row, filterValues) {
		wg.Done()
		return
	}

	b, _ := json.Marshal(row)
	match := pattern.Find(b)
	if match == nil {
		wg.Done()
		return
	}

	sortChannel <- row
}

func mergeResults(
	sortChannel chan jsonRow,
	waitGroup *sync.WaitGroup,
	pq *PriorityQueue) {
	for match := range sortChannel {
		message := (match["message"]).(map[string]interface{})
		timestamp := (message["asctime"]).(string)
		item := &Item{
			value:    match,
			priority: timestamp,
		}
		heap.Push(pq, item)
		waitGroup.Done()
	}
}

func findMatchInFile(
	pattern *regexp.Regexp,
	path string,
	wg *sync.WaitGroup,
	sortChannel chan jsonRow,
	filterValues map[string]interface{}) {

	defer wg.Done()

	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("Could not open file %s", path)
		os.Exit(1)
	}
	defer file.Close()

	// detect if the file is a
	// zlib compressed file and
	// automatically decompress
	var reader io.Reader
	// TODO(nickhil) : change this to
	// detect gzipping based on file contents
	// rather than .gz extension
	if strings.Contains(path, ".gz") {
		reader, err = gzip.NewReader(file)
		if err != nil {
			log.Fatalf(
				"Error unzipping file %s\n%s", path, err)
			os.Exit(1)
		}
	} else {
		reader = file
	}

	decoder := json.NewDecoder(reader)
	for decoder.More() {
		var r jsonRow
		err := decoder.Decode(&r)
		if err != nil {
			log.Fatalf("Could not parse %s", path)
			os.Exit(1)
		}
		wg.Add(1)
		go filterJSON(
			r, pattern, sortChannel, wg, filterValues)
	}
}

func findMatches(
	pattern *regexp.Regexp,
	waitGroup *sync.WaitGroup,
	sortChannel chan jsonRow,
	filterValues map[string]interface{}) filepath.WalkFunc {
	return func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Fatalf("Error in walking file %s\n%s", path, err)
			runtime.Goexit()
		}
		switch mode := info.Mode(); {
		case mode.IsDir():
			return nil
		case mode.IsRegular():
			waitGroup.Add(1)
			go findMatchInFile(
				pattern,
				path,
				waitGroup,
				sortChannel,
				filterValues)
		}
		return nil
	}
}

func main() {
	patternPtr := flag.String(
		"pattern",
		"",
		"Pattern to search for. (required)")

	filenamePtr := flag.String(
		"path",
		"./",
		"File or directory to search in.")

	practiceIDPtr := flag.Int(
		"practice_id",
		-1,
		"Practice ID to filter on.")

	requestIDPtr := flag.String(
		"request_id",
		"",
		"Request ID to filter on.")

	helpPtr := flag.Bool(
		"help",
		false,
		"Print help message.")

	flag.Parse()

	if *helpPtr {
		flag.PrintDefaults()
		os.Exit(0)
	}

	if *patternPtr == "" {
		fmt.Println("Please enter a non-empty string for the pattern argument.")
		flag.PrintDefaults()
		os.Exit(0)
	}

	pattern, compileErr := regexp.Compile(
		*patternPtr)

	if compileErr != nil {
		log.Fatalf("Could not compile regex %s", *patternPtr)
		os.Exit(1)
	}

	// practiceID and requiestID filters (and maybe more!)
	// stored here. If practiceID or requestID
	// are present, rows which do not match on
	// either field will be filtered out.
	filterValues := make(map[string]interface{})

	if *practiceIDPtr != -1 {
		filterValues["practice_id"] = *practiceIDPtr
	}

	if *requestIDPtr != "" {
		filterValues["request_id"] = *requestIDPtr
	}

	// a priority queue keeps our
	// results in sorted order at
	// all times. the queue receives
	// new entries via sortChannel
	queue := make(PriorityQueue, 0)
	sortChannel := make(chan jsonRow, 100)

	// a wait group blocks
	// the printing function
	// until all lines have been
	// processed. see below
	var waitGroup sync.WaitGroup

	// this goroutine continually sorts
	// rows by timestamp in the background
	go mergeResults(
		sortChannel, &waitGroup, &queue)

	// walk the directory / file recursively
	err := filepath.Walk(
		*filenamePtr,
		findMatches(
			pattern, &waitGroup, sortChannel, filterValues))

	if err != nil {
		log.Fatalf("Error walking file tree\n%s", err)
		os.Exit(1)
	}

	// blocks until all rows in all
	// files have been processed.
	waitGroup.Wait()
	close(sortChannel)

	for queue.Len() > 0 {
		item := heap.Pop(&queue).(*Item)
		value := item.value
		jsonified, _ := json.Marshal(value)
		fmt.Println(string(jsonified))
	}
}
