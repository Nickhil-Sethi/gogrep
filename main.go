package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"runtime/pprof"

	"github.com/Nickhil-Sethi/gogrep/searchrequest"
)

func main() {
	patternPtr := flag.String(
		"e",
		"",
		"Pattern to search for. (required)")

	filenamePtr := flag.String(
		"f",
		"./",
		"File or directory to search in.")

	PracticeIDPtr := flag.Int(
		"i",
		-1,
		"Practice ID to filter on.")

	RequestIDPtr := flag.String(
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

	cpuprofile := flag.String(
		"cpuprofile",
		"",
		"write cpu profile to file")

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

	ParseJSON := *jsonPtr
	if !ParseJSON && ((*PracticeIDPtr != -1) || (*RequestIDPtr != "")) {
		log.Fatal("To filter on fields, use the --json flag.")
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	// PracticeID and requiestID filters (and maybe more!)
	// stored here. If PracticeID or RequestID
	// are present, rows which do not match on
	// either field will be filtered out.
	filterValues := searchrequest.FilterObject{}

	filterValues.PracticeID = *PracticeIDPtr
	filterValues.RequestID = *RequestIDPtr

	s := searchrequest.SearchRequest{
		Pattern:      pattern,
		Path:         *filenamePtr,
		ParseJSON:    *jsonPtr,
		FilterValues: filterValues}

	results := s.FindResults()
	// encoder := json.NewEncoder(os.Stdout)
	for _, row := range results {
		// encoder.Encode(row)
		s, _ := row.GetContent()
		fmt.Printf("%s: %s\n", row.FilePath, s)
	}
}
