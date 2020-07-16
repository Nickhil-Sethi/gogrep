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

// an unfortunate hack to tolerate
// unstructured JSON
type jsonRow map[string]interface{}

type resultRow struct {
	stringContent string
	jsonContent   jsonRow
}

type filterObject struct {
	practiceID int
	requestID  string
}

type searchParameters struct {
	pattern      *regexp.Regexp
	path         string
	parseJSON    bool
	filterValues filterObject
}

func practiceIDMatches(row jsonRow, filter filterObject) bool {
	message := (row["message"]).(map[string]interface{})
	practiceID, _ := message["practice_id"]
	rowPracticeID := int(practiceID.(float64))
	filterPresent := (filter.practiceID != -1)
	if filterPresent && filter.practiceID != rowPracticeID {
		return false
	}
	return true
}

func requestIDMatches(row jsonRow, filter filterObject) bool {
	message := (row["message"]).(map[string]interface{})
	requestID, _ := message["request_id"]
	filterPresent := (filter.requestID != "")
	if filterPresent && filter.requestID != requestID {
		return false
	}
	return true
}

func rowMatchesFilters(row jsonRow, filter filterObject) bool {
	return practiceIDMatches(row, filter) && requestIDMatches(row, filter)
}

func filterRow(
	searchParams searchParameters,
	row resultRow,
	wg *sync.WaitGroup,
	sortChannel chan resultRow) {

	if searchParams.parseJSON && !rowMatchesFilters(
		row.jsonContent, searchParams.filterValues) {
		wg.Done()
		return
	}

	var rowBytes []byte
	var match []byte

	if searchParams.parseJSON {
		rowBytes, _ = json.Marshal(row.jsonContent)
	} else {
		rowBytes = []byte(row.stringContent)
	}

	match = searchParams.pattern.Find(rowBytes)
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
	searchParams searchParameters,
	filePath string,
	reader *io.Reader,
	wg *sync.WaitGroup,
	sortChannel chan resultRow) {

	decoder := json.NewDecoder(*reader)
	for decoder.More() {
		var r jsonRow
		err := decoder.Decode(&r)
		if err != nil {
			log.Fatalf("Could not parse %s", filePath)
		}
		wg.Add(1)
		row := resultRow{
			jsonContent:   r,
			stringContent: "",
		}
		go filterRow(
			searchParams,
			row,
			wg,
			sortChannel)
	}
}

func iterLinesPlain(
	searchParams searchParameters,
	filePath string,
	reader *io.Reader,
	wg *sync.WaitGroup,
	sortChannel chan resultRow) {

	scanner := bufio.NewScanner(*reader)
	for scanner.Scan() {
		line := scanner.Text()
		row := resultRow{
			jsonContent:   make(map[string]interface{}),
			stringContent: line,
		}
		wg.Add(1)
		go filterRow(
			searchParams,
			row,
			wg,
			sortChannel)
	}
}

func findMatchInFile(
	filePath string,
	searchParams searchParameters,
	waitGroup *sync.WaitGroup,
	sortChannel chan resultRow) {

	defer waitGroup.Done()

	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Could not open file %s", filePath)
	}
	defer file.Close()

	// detect if the file is a
	// zlib compressed file and
	// automatically decompress
	var reader io.Reader
	// TODO(nickhil) : change this to
	// detect gzipping based on file contents
	// rather than .gz extension
	if strings.Contains(filePath, ".gz") {
		reader, err = gzip.NewReader(file)
		if err != nil {
			log.Fatalf(
				"Error unzipping file %s\n%s", filePath, err)
		}
	} else {
		reader = file
	}

	if searchParams.parseJSON {
		iterLinesJSON(
			searchParams,
			filePath,
			&reader,
			waitGroup,
			sortChannel)
	} else {
		iterLinesPlain(
			searchParams,
			filePath,
			&reader,
			waitGroup,
			sortChannel)
	}
}

func findMatches(
	searchParams searchParameters,
	waitGroup *sync.WaitGroup,
	sortChannel chan resultRow) filepath.WalkFunc {
	return func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			log.Fatalf("Error in walking file %s\n%s", filePath, err)
		}
		switch mode := info.Mode(); {
		case mode.IsDir():
			return nil
		case mode.IsRegular():
			waitGroup.Add(1)
			go findMatchInFile(
				filePath,
				searchParams,
				waitGroup,
				sortChannel)
		}
		return nil
	}
}

func findResults(searchParams searchParameters) []string {

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
		searchParams.parseJSON)

	// walk the directory / file recursively
	err := filepath.Walk(
		searchParams.path,
		findMatches(
			searchParams,
			&waitGroup,
			sortChannel))

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
	filterValues := filterObject{}

	if *practiceIDPtr != -1 {
		filterValues.practiceID = *practiceIDPtr
	}

	if *requestIDPtr != "" {
		filterValues.requestID = *requestIDPtr
	}

	searchParams := searchParameters{
		pattern:   pattern,
		path:      *filenamePtr,
		parseJSON: *jsonPtr,
	}
	results := findResults(searchParams)

	for row := range results {
		fmt.Println(row)
	}
}
