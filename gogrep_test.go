package main

import (
	"regexp"
	"testing"
)

func TestGoGrepIt(t *testing.T) {
	filter := make(map[string]interface{})
	goGrepIt(
		"./test/",
		regexp.MustCompile("vulture"),
		filter)
}
