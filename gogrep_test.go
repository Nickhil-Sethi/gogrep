package main

import (
	"regexp"
	"testing"
)

func TestGoGrep(t *testing.T) {
	filter := make(map[string]interface{})
	goGrepIt(
		"./test/",
		regexp.MustCompile("vulture"),
		filter)
}
