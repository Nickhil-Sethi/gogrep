package searchrequest

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"container/heap"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
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
	pq           *priorityQueue
}

func PracticeIDMatches(row jsonRow, filter FilterObject) bool {
	message := (row["message"]).(map[string]interface{})
	PracticeID, _ := message["practice_id"]
	rowPracticeID := int(PracticeID.(float64))
	filterPresent := (filter.PracticeID != -1)
	if filterPresent && filter.PracticeID != rowPracticeID {
		return false
	}
	return true
}

func RequestIDMatches(row jsonRow, filter FilterObject) bool {
	message := (row["message"]).(map[string]interface{})
	RequestID, _ := message["request_id"]
	filterPresent := (filter.RequestID != "")
	if filterPresent && filter.RequestID != RequestID {
		return false
	}
	return true
}

func rowMatchesFilters(row jsonRow, filter FilterObject) bool {
	return PracticeIDMatches(row, filter) && RequestIDMatches(row, filter)
}

func (s *SearchRequest) filterRow(row ResultRow) {

	if s.ParseJSON && !rowMatchesFilters(
		row.jsonContent, s.FilterValues) {
		s.waitGroup.Done()
		return
	}

	var rowBytes []byte
	var match []byte

	if s.ParseJSON {
		rowBytes, _ = json.Marshal(row.jsonContent)
	} else {
		rowBytes = []byte(row.stringContent)
	}

	match = s.Pattern.Find(rowBytes)
	if match == nil {
		s.waitGroup.Done()
		return
	}

	s.sortChannel <- row
}

func (s *SearchRequest) mergeResults() {
	for match := range s.sortChannel {
		var priority string
		if s.ParseJSON {
			message := (match.jsonContent["message"]).(map[string]interface{})
			priority = (message["asctime"]).(string)
		} else {
			priority = match.stringContent
		}
		item := &item{
			value:    match,
			priority: priority,
		}
		heap.Push(s.pq, item)
		s.waitGroup.Done()
	}
}

func (s *SearchRequest) iterLinesJSON(
	filePath string,
	reader *io.Reader) {

	decoder := json.NewDecoder(*reader)
	for decoder.More() {
		var r jsonRow
		err := decoder.Decode(&r)
		if err != nil {
			log.Fatalf("Could not parse %s", filePath)
		}
		s.waitGroup.Add(1)
		// fmt.Print(r)
		row := ResultRow{
			jsonContent:   r,
			stringContent: "",
		}
		go s.filterRow(
			row)
	}
}

func (s *SearchRequest) iterLinesPlain(
	filePath string,
	reader *io.Reader) {

	scanner := bufio.NewScanner(*reader)
	for scanner.Scan() {
		line := scanner.Text()
		row := ResultRow{
			jsonContent:   make(map[string]interface{}),
			stringContent: line,
			IsJSON:        s.ParseJSON,
		}
		s.waitGroup.Add(1)
		go s.filterRow(
			row)
	}
}

func (s *SearchRequest) findMatchInFile(
	filePath string) {

	defer s.waitGroup.Done()

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

	if s.ParseJSON {
		s.iterLinesJSON(
			filePath,
			&reader)
	} else {
		s.iterLinesPlain(
			filePath,
			&reader)
	}
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
			go s.findMatchInFile(
				filePath)
		}
		return nil
	}
}

// FindResults returns results of executed query
func (s *SearchRequest) FindResults() []ResultRow {

	queue := make(priorityQueue, 0)
	sortChannel := make(chan ResultRow, ChannelSize)
	var waitGroup sync.WaitGroup

	s.pq = &queue
	s.sortChannel = sortChannel
	s.waitGroup = &waitGroup

	// this goroutine continually sorts
	// rows by timestamp in the background
	go s.mergeResults()

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

	results := []ResultRow{}

	for s.pq.Len() > 0 {
		item := heap.Pop(s.pq).(*item)
		value := item.value
		results = append(results, value)
	}
	return results
}
