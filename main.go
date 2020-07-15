package main

import (
	"bufio"
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
	"strings"
	"sync"
)

type jsonRow map[string]interface{}

type resultRow struct {
	stringContent string
	jsonContent   jsonRow
}

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

func filterRow(
	row resultRow,
	pattern *regexp.Regexp,
	sortChannel chan resultRow,
	wg *sync.WaitGroup,
	parseJSON bool,
	filterValues map[string]interface{}) {

	if parseJSON && !rowMatchesFilters(row.jsonContent, filterValues) {
		wg.Done()
		return
	}

	var rowBytes []byte
	var match []byte

	if parseJSON {
		rowBytes, _ = json.Marshal(row.jsonContent)
	} else {
		rowBytes = []byte(row.stringContent)
	}

	match = pattern.Find(rowBytes)
	if match == nil {
		wg.Done()
		return
	}

	sortChannel <- row
}

func mergeResults(
	sortChannel chan resultRow,
	waitGroup *sync.WaitGroup,
	pq *PriorityQueue,
	parseJSON bool) {
	for match := range sortChannel {
		message := (match.jsonContent["message"]).(map[string]interface{})
		// case to time
		timestamp := (message["asctime"]).(string)
		item := &Item{
			value:    match,
			priority: timestamp,
		}
		heap.Push(pq, item)
		waitGroup.Done()
	}
}

func iterLinesJSON(
	reader *io.Reader,
	path string,
	pattern *regexp.Regexp,
	wg *sync.WaitGroup,
	sortChannel chan resultRow,
	filterValues map[string]interface{}) {

	decoder := json.NewDecoder(*reader)
	for decoder.More() {
		var r jsonRow
		err := decoder.Decode(&r)
		if err != nil {
			log.Fatalf("Could not parse %s", path)
		}
		wg.Add(1)
		row := resultRow{
			jsonContent:   r,
			stringContent: "",
		}
		go filterRow(
			row,
			pattern,
			sortChannel,
			wg,
			true,
			filterValues)
	}
}

func iterLinesPlain(
	reader *io.Reader,
	path string,
	pattern *regexp.Regexp,
	wg *sync.WaitGroup,
	sortChannel chan resultRow,
	filterValues map[string]interface{}) {

	scanner := bufio.NewScanner(*reader)
	for scanner.Scan() {
		line := scanner.Text()
		row := resultRow{
			jsonContent:   make(map[string]interface{}),
			stringContent: line,
		}
		wg.Add(1)
		go filterRow(
			row,
			pattern,
			sortChannel,
			wg,
			false,
			filterValues)
	}
}

func findMatchInFile(
	pattern *regexp.Regexp,
	path string,
	wg *sync.WaitGroup,
	sortChannel chan resultRow,
	parseJSON bool,
	filterValues map[string]interface{}) {

	defer wg.Done()

	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("Could not open file %s", path)
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
		}
	} else {
		reader = file
	}

	if parseJSON {
		iterLinesJSON(
			&reader,
			path,
			pattern,
			wg,
			sortChannel,
			filterValues)
	} else {
		iterLinesPlain(
			&reader,
			path,
			pattern,
			wg,
			sortChannel,
			filterValues)
	}
}

func findMatches(
	pattern *regexp.Regexp,
	waitGroup *sync.WaitGroup,
	sortChannel chan resultRow,
	parseJSON bool,
	filterValues map[string]interface{}) filepath.WalkFunc {
	return func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Fatalf("Error in walking file %s\n%s", path, err)
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
				parseJSON,
				filterValues)
		}
		return nil
	}
}

func goGrepIt(
	filename string,
	pattern *regexp.Regexp,
	parseJSON bool,
	filterValues map[string]interface{}) []string {

	// a priority queue keeps our
	// results in sorted order at
	// all times. the queue receives
	// new entries via sortChannel
	queue := make(PriorityQueue, 0)
	sortChannel := make(chan resultRow, 100)

	// a wait group blocks
	// the printing function
	// until all lines have been
	// processed. see below
	var waitGroup sync.WaitGroup

	// this goroutine continually sorts
	// rows by timestamp in the background
	go mergeResults(
		sortChannel,
		&waitGroup,
		&queue,
		parseJSON)

	// walk the directory / file recursively
	err := filepath.Walk(
		filename,
		findMatches(
			pattern,
			&waitGroup,
			sortChannel,
			parseJSON,
			filterValues))

	if err != nil {
		log.Fatalf("Error walking file tree\n%s", err)
	}

	// blocks until all rows in all
	// files have been processed.
	waitGroup.Wait()
	close(sortChannel)

	results := []string{}

	for queue.Len() > 0 {
		item := heap.Pop(&queue).(*Item)
		value := item.value
		jsonified, parseErr := json.Marshal(value)
		if parseErr != nil {
			log.Fatalf("Something went wrong. Error parsing JSON from heap.")
		}
		results = append(results, string(jsonified))
	}
	return results
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

	jsonPtr := flag.Bool(
		"json",
		false,
		"Parse file as newline separated json.")

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
	}

	parseJSON := *jsonPtr
	if !parseJSON && ((*practiceIDPtr != -1) || (*requestIDPtr != "")) {
		log.Fatal("To filter on fields, use the --json flag.")
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

	results := goGrepIt(
		*filenamePtr,
		pattern,
		parseJSON,
		filterValues)

	for row := range results {
		fmt.Println(row)
	}
}
