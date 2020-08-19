package searchrequest

import (
	"bytes"
	"container/heap"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sync"
)

const ChannelSize = 100

// an unfortunate hack to tolerate
// unstructured JSON
type jsonRow map[string]interface{}

func (j jsonRow) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString("{")
	length := len(j)
	count := 0
	for key, value := range j {
		switch value.(type) {
		case int:
			buffer.WriteString(fmt.Sprintf("\"%s\":%d", key, value))
			count++
			if count < length {
				buffer.WriteString(",")
			}
		case string:
			buffer.WriteString(fmt.Sprintf("\"%s\":%s", key, value))
			count++
			if count < length {
				buffer.WriteString(",")
			}
		default:
			jsonified, err := json.Marshal(value)
			if err != nil {
				return nil, err
			}
			add := fmt.Sprintf("\"%s\":%s", key, jsonified)
			buffer.WriteString(add)
			count = count + len(add)
			if count < length {
				buffer.WriteString(",")
			}
		}
	}
	buffer.WriteString("}")
	return buffer.Bytes(), nil
}

// ResultRow : represents one match
// of resulting search
type ResultRow struct {
	stringContent string
	jsonContent   jsonRow
	FilePath      string
	IsJSON        bool
}

type FilterObject struct {
	PracticeID int
	RequestID  string
}

// SearchRequest : interface of search query
type SearchRequest struct {
	Pattern      *regexp.Regexp
	Path         string
	ParseJSON    bool
	FilterValues FilterObject
	waitGroup    *sync.WaitGroup
	sortChannel  chan ResultRow
	rowChannel   chan ResultRow
	fileChannel  chan string
	pq           *priorityQueue
}

func (s *SearchRequest) findMatches() filepath.WalkFunc {
	return func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			log.Fatalf("Error in walking file %s\n%s", filePath, err)
		}
		switch mode := info.Mode(); {
		case mode.IsDir():
			return nil
		case mode.IsRegular():
			s.waitGroup.Add(1)
			s.fileChannel <- filePath
		}
		return nil
	}
}

func (s *SearchRequest) setupFileWorkers() {
	for i := 0; i < runtime.GOMAXPROCS(-1); i++ {
		fworker := fileWorker{s}
		go fworker.run()
	}
}

func (s *SearchRequest) setupRowWorkers() {
	for i := 0; i < runtime.GOMAXPROCS(-1); i++ {
		lworker := rowWorker{s}
		go lworker.run()
	}
}

func (s *SearchRequest) initialize() {
	queue := make(priorityQueue, 0)
	sortChannel := make(chan ResultRow, ChannelSize)
	rowChannel := make(chan ResultRow, ChannelSize)
	fileChannel := make(chan string, ChannelSize)
	var waitGroup sync.WaitGroup

	s.pq = &queue
	s.sortChannel = sortChannel
	s.rowChannel = rowChannel
	s.fileChannel = fileChannel
	s.waitGroup = &waitGroup
}

// FindResults returns results of executed query
func (s *SearchRequest) FindResults() []ResultRow {
	s.initialize()

	// this worker sorts the results in the
	// background
	sortWorker := sortWorker{s}
	go sortWorker.run()

	// two worker pools,
	// one for files and
	// one for lines.
	// this bounds the memory usage
	// of the program when used
	// over large file trees
	s.setupFileWorkers()
	s.setupRowWorkers()

	// walk the directory / file recursively
	err := filepath.Walk(
		s.Path,
		s.findMatches())

	if err != nil {
		log.Fatalf("Error walking file tree\n%s", err)
	}

	// blocks until all rows in all
	// files have been processed.
	s.waitGroup.Wait()
	close(s.sortChannel)
	close(s.fileChannel)
	close(s.rowChannel)

	results := []ResultRow{}
	for s.pq.Len() > 0 {
		item := heap.Pop(s.pq).(*item)
		value := item.value
		results = append(results, value)
	}
	return results
}
