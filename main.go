package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
)

type jsonRow map[string]interface{}

func filterJSON(
	row jsonRow,
	pattern *regexp.Regexp,
	sortChannel chan jsonRow) {
	b, _ := json.Marshal(row)
	match := pattern.Find(b)
	if match != nil {
		sortChannel <- row
	}
}

func mergeResults(
	sortChannel chan jsonRow) {
	for match := range sortChannel {
		fmt.Println(match)
	}
}

func findMatchInFile(
	pattern *regexp.Regexp,
	path string,
	waitGroup *sync.WaitGroup,
	sortChannel chan jsonRow) {
	defer waitGroup.Done()

	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	dec := json.NewDecoder(file)
	for dec.More() {
		var r jsonRow
		err := dec.Decode(&r)
		if err != nil {
			panic(err)
		}
		go filterJSON(r, pattern, sortChannel)
	}
}

func findMatches(
	pattern *regexp.Regexp,
	waitGroup *sync.WaitGroup,
	sortChannel chan jsonRow) filepath.WalkFunc {
	return func(path string, info os.FileInfo, err error) error {
		if err != nil {
			panic(err)
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
				sortChannel)
		}
		return nil
	}
}

func main() {
	patternPtr := flag.String(
		"pattern",
		"",
		"Pattern to search for")

	filenamePtr := flag.String(
		"path",
		"./",
		"File or directory to search in. Defaults to current directory.")

	filterJsonPath := flag.String(
		"key",
		"",
		"JSON path of key to filter on.")

	filterJsonValue := flag.String(
		"value",
		"",
		"Key value to filter on.")

	flag.Parse()

	if *patternPtr == "" {
		fmt.Println("Please enter a non-empty string for the pattern argument.")
		return
	}

	eitherJSONProvided := (*filterJsonPath != "" || *filterJsonValue != "")
	bothJSONProvided := (*filterJsonPath != "" && *filterJsonValue != "")
	if eitherJSONProvided && !bothJSONProvided {
		fmt.Println("Must provide both filter key and filter value if using filter functionality.")
	}
	fmt.Println("Searching for ", *patternPtr, " in ", *filenamePtr)

	sortChannel := make(chan jsonRow)

	var waitGroup sync.WaitGroup
	pattern := regexp.MustCompile(*patternPtr)
	err := filepath.Walk(
		*filenamePtr,
		findMatches(
			pattern, &waitGroup, sortChannel))

	if err != nil {
		panic(err)
	}

	go mergeResults(sortChannel)

	waitGroup.Wait()
}
