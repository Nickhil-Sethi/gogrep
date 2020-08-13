package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"sync"
	"testing"
)

func TestFindResults(t *testing.T) {
	queue := make(PriorityQueue, 0)
	sortChannel := make(chan resultRow, 100)
	var waitGroup sync.WaitGroup

	s := SearchRequest{
		path:      "./test",
		pattern:   regexp.MustCompile("captain"),
		parseJSON: true,
		filterValues: filterObject{
			requestID:  "",
			practiceID: -1,
		},
		waitGroup:   &waitGroup,
		pq:          &queue,
		sortChannel: sortChannel,
	}
	results := s.findResults()

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
