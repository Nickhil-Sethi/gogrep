package main

import (
	"container/heap"
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
	sortChannel chan jsonRow,
	waitGroup *sync.WaitGroup,
	pq *PriorityQueue) {
	for match := range sortChannel {
		parsedMatch, _ := json.Marshal(match)
		// TODO(nickhil) : add heap
		// priority queue here
		fmt.Println(string(parsedMatch))
		waitGroup.Done()
	}
}

func findMatchInFile(
	pattern *regexp.Regexp,
	path string,
	wg *sync.WaitGroup,
	sortChannel chan jsonRow) {
	defer wg.Done()

	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// TODO(nickhil) : add
	// gzip functionality here

	dec := json.NewDecoder(file)
	for dec.More() {
		var r jsonRow
		err := dec.Decode(&r)
		if err != nil {
			panic(err)
		}
		wg.Add(1)
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

	// TODO(nickhil): add real options here
	// remove json filter value
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

	sortChannel := make(chan jsonRow, 100)
	var waitGroup sync.WaitGroup
	queue := make(PriorityQueue, 0)

	go mergeResults(
		sortChannel, &waitGroup, &queue)

	pattern := regexp.MustCompile(*patternPtr)
	err := filepath.Walk(
		*filenamePtr,
		findMatches(
			pattern, &waitGroup, sortChannel))

	if err != nil {
		panic(err)
	}

	waitGroup.Wait()
	close(sortChannel)

	// Take the items out; they arrive in decreasing priority order.
	for queue.Len() > 0 {
		item := heap.Pop(&queue).(*Item)
		fmt.Printf("%.2d:%s ", item.priority, item.value)
	}
}
