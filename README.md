# gogrep
Fast parallel file search implemented in Golang.

## Design and Implementation
Through the magic of the golang runtime, gogrep outperforms the native `grep` utility by using all cores simultaneously, parallelizing search accross each line in each file. (source?)

More explicitly, gogrep uses goroutines to parallelize file search over a set of files. Given a set of search criteria (i.e. a regex, and optional practice id and request id), and a file path, and recursively walks the subtree searching each file for lines matching the search criteria. 

Gogrep spins up one goroutine for each file to be processed, one for each line in each file, and a goroutine which continually sorts the results in the background. After all lines are processed, gogrep prints all lines matched, ordered by timestamp.

## Build and Run
```
$ cd gogrep
$ go build .
```
The directory should now contain a `gogrep` binary, which you can run against the test directory like so:
```
$ ls 
README.md       gogrep          main.go         pq.go           test
$ ./gogrep --pattern "hulk" --path ./test/
{ "message":{ "asctime":"2020-05-03 19:30:12,882", "message":"hulk" "practice_id":1204712973, "request_id":"887449ef-4c93-863c-03a503a227fc",
"user_id":919888959 } }
```

## TODO

More helpful error handling. For example we see this when given a directory which can't be parsed.

```panic: invalid character 'r' looking for beginning of value``` 

Option to print filenames
Option to pretty print json
Flexible json filtering like jq
Benchmark against linux kernel source tree
