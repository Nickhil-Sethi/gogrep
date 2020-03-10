package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sync"
)

func mergeResults(
	matchChannel chan string) {
	for match := range matchChannel {
		fmt.Println("Found match: ", string(match))
	}
}

// TODO(nickhil) : have this read line by line
func findMatchInFile(
	pattern *regexp.Regexp,
	path string,
	waitGroup *sync.WaitGroup,
	matchChannel chan string) {

	defer waitGroup.Done()

	dat, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}

	match := pattern.Find(dat)
	if match != nil {
		matchChannel <- string(match)
	}

}

func findMatches(
	pattern *regexp.Regexp,
	waitGroup *sync.WaitGroup,
	matchChannel chan string) filepath.WalkFunc {
	return func(path string, info os.FileInfo, err error) error {
		if err != nil {
			panic(err)
		}
		switch mode := info.Mode(); {
		case mode.IsDir():
			// do directory stuff
			return nil
		case mode.IsRegular():
			// do file stuff
			waitGroup.Add(1)
			go findMatchInFile(pattern, path, waitGroup, matchChannel)
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
	matchChannel := make(chan string)

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
