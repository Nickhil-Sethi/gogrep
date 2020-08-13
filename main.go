package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"sync"

	"github.com/Nickhil-Sethi/gogrep/searchrequest"
)

func main() {
	patternPtr := flag.String(
		"p",
		"",
		"Pattern to search for. (required)")

	filenamePtr := flag.String(
		"d",
		"./",
		"File or directory to search in.")

	practiceIDPtr := flag.Int(
		"i",
		-1,
		"Practice ID to filter on.")

	requestIDPtr := flag.String(
		"r",
		"",
		"Request ID to filter on.")

	helpPtr := flag.Bool(
		"h",
		false,
		"Print help message.")

	jsonPtr := flag.Bool(
		"j",
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

	filterValues.practiceID = *practiceIDPtr
	filterValues.requestID = *requestIDPtr

	queue := make(PriorityQueue, 0)
	sortChannel := make(chan resultRow, 100)
	var waitGroup sync.WaitGroup

	s := searchrequest.SearchRequest{
		pattern:      pattern,
		path:         *filenamePtr,
		parseJSON:    *jsonPtr,
		filterValues: filterValues,
		waitGroup:    &waitGroup,
		sortChannel:  sortChannel,
		pq:           &queue}

	results := s.findResults()
	// encoder := json.NewEncoder(os.Stdout)
	for _, row := range results {
		// encoder.Encode(row)
		fmt.Println(row)
	}
}
