package searchrequest

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"io"
	"log"
	"os"
	"strings"
)

type worker interface {
	run()
}

type fileWorker struct {
	*SearchRequest
}

func (w *fileWorker) run() {
	for filePath := range w.fileChannel {
		w.findMatchInFile(filePath)
	}
}

func (w *fileWorker) iterLinesJSON(
	filePath string,
	reader *io.Reader) {

	decoder := json.NewDecoder(*reader)
	for decoder.More() {
		var r jsonRow
		err := decoder.Decode(&r)
		if err != nil {
			log.Fatalf("Could not parse %s", filePath)
		}
		w.waitGroup.Add(1)
		row := ResultRow{
			jsonContent:   r,
			stringContent: "",
		}
		w.rowChannel <- row
	}
}

func (w *fileWorker) iterLinesPlain(
	filePath string,
	reader *io.Reader) {

	scanner := bufio.NewScanner(*reader)
	for scanner.Scan() {
		line := scanner.Text()
		row := ResultRow{
			jsonContent:   make(map[string]interface{}),
			stringContent: line,
			IsJSON:        w.ParseJSON,
		}
		w.waitGroup.Add(1)
		w.rowChannel <- row
	}
}

func (w *fileWorker) findMatchInFile(
	filePath string) {

	defer w.waitGroup.Done()

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

	if w.ParseJSON {
		w.iterLinesJSON(
			filePath,
			&reader)
	} else {
		w.iterLinesPlain(
			filePath,
			&reader)
	}
}

type rowWorker struct {
	*SearchRequest
}

func (w *rowWorker) run() {
	for row := range w.rowChannel {
		w.filterRow(row)
	}
}

func practiceIDMatches(row jsonRow, filter FilterObject) bool {
	message := (row["message"]).(map[string]interface{})
	PracticeID, _ := message["practice_id"]
	rowPracticeID := int(PracticeID.(float64))
	filterPresent := (filter.PracticeID != -1)
	if filterPresent && filter.PracticeID != rowPracticeID {
		return false
	}
	return true
}

func requestIDMatches(row jsonRow, filter FilterObject) bool {
	message := (row["message"]).(map[string]interface{})
	RequestID, _ := message["request_id"]
	filterPresent := (filter.RequestID != "")
	if filterPresent && filter.RequestID != RequestID {
		return false
	}
	return true
}

func rowMatchesFilters(row jsonRow, filter FilterObject) bool {
	return practiceIDMatches(row, filter) && requestIDMatches(row, filter)
}

func (w *rowWorker) filterRow(row ResultRow) {

	if w.ParseJSON && !rowMatchesFilters(
		row.jsonContent, w.FilterValues) {
		w.waitGroup.Done()
		return
	}

	var rowBytes []byte
	var match []byte

	if w.ParseJSON {
		rowBytes, _ = json.Marshal(row.jsonContent)
	} else {
		rowBytes = []byte(row.stringContent)
	}

	match = w.Pattern.Find(rowBytes)
	if match == nil {
		w.waitGroup.Done()
		return
	}

	w.sortChannel <- row
}
