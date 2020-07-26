package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"testing"
)

func TestFindResults(t *testing.T) {

	searchParams := searchParameters{
		path:      "./test",
		pattern:   regexp.MustCompile("vulture"),
		parseJSON: true,
		filterValues: filterObject{
			requestID:  "",
			practiceID: -1,
		},
	}
	results := findResults(searchParams)

	m := map[string]interface{}{"message": map[string]interface{}{
		"asctime":     "2020-05-03 11:10:12,112",
		"request_id":  "687449ef-4c93-863c-03a503a227fc",
		"practice_id": 1204712973,
		"user_id":     919888959,
		"message":     "vulture",
	}}
	strM, _ := json.Marshal(m)
	var exp [1]string
	exp[0] = string(strM)
	if !reflect.DeepEqual(results, exp) {
		fmt.Print("Unexpected results ", results, exp)
		t.Fail()
	}
}
