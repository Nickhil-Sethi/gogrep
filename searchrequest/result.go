package searchrequest

import (
	"bytes"
	"encoding/json"
	"fmt"
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

// ResultRow : represents one match
// of resulting search
type ResultRow struct {
	stringContent string
	jsonContent   jsonRow
	FilePath      string
	IsJSON        bool
}

func (r *ResultRow) GetContent() (string, error) {
	if r.IsJSON {
		contentBytes, err := json.Marshal(r.jsonContent)
		if err != nil {
			return "", err
		}
		return string(contentBytes), nil
	}
	return r.stringContent, nil
}
