package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"testing"

	"github.com/Nickhil-Sethi/gogrep/searchrequest"
)

func TestFindResults(t *testing.T) {

	s := searchrequest.SearchRequest{
		Path:      "./test",
		Pattern:   regexp.MustCompile("captain"),
		ParseJSON: true,
		FilterValues: searchrequest.FilterObject{
			RequestID:  "",
			PracticeID: -1,
		},
	}
	results := s.FindResults()

	m := map[string]interface{}{"message": map[string]interface{}{
		"asctime":     "2020-05-03 13:10:12,112",
		"request_id":  "687449ef-4c93-863c-03a503a227fc",
		"practice_id": 1204712973,
		"user_id":     919888959,
		"message":     "captain america",
	}}
	expectedBytes, _ := json.Marshal(m)
	expectedString := string(expectedBytes)

	receivedString, _ := results[0].GetContent()

	if len(results) == 0 {
		fmt.Print("Empty results list returned. Want one element.")
		t.Fail()
	}

	if !results[0].IsJSON {
		fmt.Printf(
			"Wrong value of IsJSON received. Have %t / Want %t",
			results[0].IsJSON,
			true)
		t.Fail()
	}

	if !reflect.DeepEqual(receivedString, expectedString) {
		fmt.Printf("Unexpected content. Have %s / Want %s ", receivedString, expectedString)
		t.Fail()
	}

	if !(results[0].FilePath == "test/test_data_2.json") {
		fmt.Printf("Unexpected FilePath. Have %s / Want %s", results[0].FilePath, "test/test_data_2.json")
		t.Fail()
	}
}
