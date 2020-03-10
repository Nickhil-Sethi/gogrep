package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sync"
)

func mergeResults(
	matchChannel chan []string,
	waitGroup *sync.WaitGroup) {
	panic("Not implemented!")
}

// TODO(nickhil) : have this read line by line
func findMatchInFile(
	pattern string,
	path string,
	waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	dat, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}
	re := regexp.MustCompile(pattern)
	match := re.Find(dat)
	if match != nil {
		fmt.Println("Found match: ", string(match), " in ", path)
	}
}

func findMatches(
	pattern string,
	waitGroup *sync.WaitGroup) filepath.WalkFunc {
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
			go findMatchInFile(pattern, path, waitGroup)
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
	fmt.Println("Searching for ", *patternPtr, " in ", *filenamePtr)
	fmt.Println("GOMAXPROCS: ", runtime.NumCPU())
	var waitGroup sync.WaitGroup
	err := filepath.Walk(
		*filenamePtr,
		findMatches(
			*patternPtr, &waitGroup))

	if err != nil {
		panic(err)
	}

	waitGroup.Wait()
}
