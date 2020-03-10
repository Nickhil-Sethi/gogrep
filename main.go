package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
)

func mergeResults(
	matchChannel chan []byte) {
	for match := range matchChannel {
		fmt.Println(string(match))
	}
}

func findMatchInFile(
	pattern *regexp.Regexp,
	path string,
	waitGroup *sync.WaitGroup,
	matchChannel chan []byte) {
	defer waitGroup.Done()

	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// TODO(nickhil) : we're doing a
	// log of casting here. is this expensive?
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Bytes()
		match := pattern.Find(line)
		if match != nil {
			matchChannel <- line
		}
	}
}

func findMatches(
	pattern *regexp.Regexp,
	waitGroup *sync.WaitGroup,
	matchChannel chan []byte) filepath.WalkFunc {
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
				matchChannel)
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
		"File or directory to search in")

	flag.Parse()

	if *patternPtr == "" {
		fmt.Println("Please enter a non-empty string for the pattern argument.")
		return
	}

	fmt.Println("Searching for ", *patternPtr, " in ", *filenamePtr)

	var waitGroup sync.WaitGroup
	pattern := regexp.MustCompile(*patternPtr)
	matchChannel := make(chan []byte)

	err := filepath.Walk(
		*filenamePtr,
		findMatches(
			pattern, &waitGroup, matchChannel))

	if err != nil {
		panic(err)
	}

	go mergeResults(matchChannel)
	waitGroup.Wait()
}
