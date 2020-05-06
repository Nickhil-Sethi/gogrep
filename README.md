# gogrep
Regex matching tool implemented in Golang.

## Implementation


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
{
    "message":{
        "asctime":"2020-05-03 19:30:12,882",
        "message":"hulk","practice_id":1204712973,
        "request_id":"887449ef-4c93-863c-03a503a227fc",
        "user_id":919888959
    }
}
```
