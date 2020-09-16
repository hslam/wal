# wal
Package wal implements write-ahead logging.

## Feature
* Low Memory Usage
* Segment
* Batch Write
* Clean / Truncate / Reset

## Get started

### Install
```
go get github.com/hslam/wal
```
### Import
```
import "github.com/hslam/wal"
```
### Usage
#### Example
```
package main

import (
	"fmt"
	"github.com/hslam/wal"
	"os"
)

func main() {
	filepath := "wal"
	os.RemoveAll(filepath)
	log, err := wal.OpenWithOptions(filepath, &wal.Options{SegmentEntries: 3})
	if err != nil {
		panic(err)
	}
	defer log.Close()
	// Write
	log.Write(1, []byte("Hello World"))
	log.FlushAndSync()
	// Batch Write
	log.Write(2, []byte("Hello WAL"))
	log.Write(3, []byte("Hello MH"))
	log.FlushAndSync()
	data, _ := log.Read(1)
	fmt.Println(string(data))
	log.Clean(2)
	log.Truncate(2)
	log.Reset()
}
```

### Output
```
Hello World
```

### License
This package is licensed under a MIT license (Copyright (c) 2020 Meng Huang)


### Authors
wal was written by Meng Huang.


