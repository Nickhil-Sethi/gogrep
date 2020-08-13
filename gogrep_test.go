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
	strM, _ := json.Marshal(m)
	var exp [1]string
	exp[0] = string(strM)
	if !reflect.DeepEqual(results, exp) {
		fmt.Print("Unexpected results ", results, exp)
		t.Fail()
	}
}
