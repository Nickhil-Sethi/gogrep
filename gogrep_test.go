package main

import (
	"regexp"
	"testing"
)

func TestGoGrep(t *testing.T) {
	f := make(map[string]interface{})
	goGrepIt("./test/data/", regexp.MustCompile("vulture"), f)
}
