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

type ResultRow struct {
	stringContent string
	jsonContent   jsonRow
}

type filterObject struct {
	practiceID int
	requestID  string
}

type SearchRequest struct {
	pattern      *regexp.Regexp
	path         string
	parseJSON    bool
	filterValues filterObject
	waitGroup    *sync.WaitGroup
	sortChannel  chan resultRow
	pq           *PriorityQueue
}

func practiceIDMatches(row jsonRow, filter filterObject) bool {
	message := (row["message"]).(map[string]interface{})
	practiceID, _ := message["practice_id"]
	rowPracticeID := int(practiceID.(float64))
	filterPresent := (filter.practiceID != -1)
	if filterPresent && filter.practiceID != rowPracticeID {
		return false
	}
	return true
}

func requestIDMatches(row jsonRow, filter filterObject) bool {
	message := (row["message"]).(map[string]interface{})
	requestID, _ := message["request_id"]
	filterPresent := (filter.requestID != "")
	if filterPresent && filter.requestID != requestID {
		return false
	}
	return true
}

func rowMatchesFilters(row jsonRow, filter filterObject) bool {
	return practiceIDMatches(row, filter) && requestIDMatches(row, filter)
}

func (s *searchRequest) filterRow(row resultRow) {

	if s.parseJSON && !rowMatchesFilters(
		row.jsonContent, s.filterValues) {
		s.waitGroup.Done()
		return
	}

	var rowBytes []byte
	var match []byte

	if s.parseJSON {
		rowBytes, _ = json.Marshal(row.jsonContent)
	} else {
		rowBytes = []byte(row.stringContent)
	}

	match = s.pattern.Find(rowBytes)
	if match == nil {
		s.waitGroup.Done()
		return
	}

	s.sortChannel <- row
}

func (s *searchRequest) mergeResults() {
	for match := range s.sortChannel {
		var priority string
		if s.parseJSON {
			message := (match.jsonContent["message"]).(map[string]interface{})
			priority = (message["asctime"]).(string)
		} else {
			priority = match.stringContent
		}
		item := &Item{
			value:    match,
			priority: priority,
		}
		heap.Push(s.pq, item)
		s.waitGroup.Done()
	}
}

func (s *searchRequest) iterLinesJSON(
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
		row := resultRow{
			jsonContent:   r,
			stringContent: "",
		}
		go s.filterRow(
			row)
	}
}

func (s *searchRequest) iterLinesPlain(
	filePath string,
	reader *io.Reader) {

	scanner := bufio.NewScanner(*reader)
	for scanner.Scan() {
		line := scanner.Text()
		row := resultRow{
			jsonContent:   make(map[string]interface{}),
			stringContent: line,
		}
		s.waitGroup.Add(1)
		go s.filterRow(
			row)
	}
}

func (s *searchRequest) findMatchInFile(
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

	if s.parseJSON {
		s.iterLinesJSON(
			filePath,
			&reader)
	} else {
		s.iterLinesPlain(
			filePath,
			&reader)
	}
}

func (s *searchRequest) findMatches() filepath.WalkFunc {
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

func (s *searchRequest) findResults() []string {

	// this goroutine continually sorts
	// rows by timestamp in the background
	go s.mergeResults()

	// walk the directory / file recursively
	err := filepath.Walk(
		s.path,
		s.findMatches())

	if err != nil {
		log.Fatalf("Error walking file tree\n%s", err)
	}

	// blocks until all rows in all
	// files have been processed.
	s.waitGroup.Wait()
	close(s.sortChannel)

	results := []string{}

	for s.pq.Len() > 0 {
		item := heap.Pop(s.pq).(*Item)
		value := item.value
		var jsonified []byte
		if s.parseJSON {
			var parseErr error
			jsonified, parseErr = json.Marshal(value.jsonContent)
			if parseErr != nil {
				log.Fatalf("Something went wrong. Error parsing JSON from heap.")
			}
		} else {
			jsonified = []byte(value.stringContent)
		}
		results = append(results, string(jsonified))
	}
	return results
}