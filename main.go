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
	sortChannel chan jsonRow,
	wg *sync.WaitGroup) {
	b, _ := json.Marshal(row)
	match := pattern.Find(b)
	if match != nil {
		sortChannel <- row
	} else {
		wg.Done()
	}
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
		go filterJSON(r, pattern, sortChannel, wg)
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
	flag.Parse()

	if *patternPtr == "" {
		fmt.Println("Please enter a non-empty string for the pattern argument.")
		return
	}

	queue := make(PriorityQueue, 0)
	sortChannel := make(chan jsonRow, 100)

	// blocks until all rows
	// from all files have been added
	// to the heap
	var waitGroup sync.WaitGroup

	// continually sorting
	// the results in the backgroud
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

	for queue.Len() > 0 {
		item := heap.Pop(&queue).(*Item)
		value := item.value
		jsonified, _ := json.Marshal(value)
		fmt.Println(string(jsonified))
	}
}
