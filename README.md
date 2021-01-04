# wal
[![PkgGoDev](https://pkg.go.dev/badge/github.com/hslam/wal)](https://pkg.go.dev/github.com/hslam/wal)
[![Build Status](https://github.com/hslam/wal/workflows/build/badge.svg)](https://github.com/hslam/wal/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/hslam/wal?v=7e100)](https://goreportcard.com/report/github.com/hslam/wal)
[![LICENSE](https://img.shields.io/github/license/hslam/wal.svg?style=flat-square)](https://github.com/hslam/wal/blob/master/LICENSE)

Package wal implements write-ahead logging.

## Feature
* Low memory usage
* Segment
* Batch writes
* Clean/Truncate/Reset

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
```go
package main

import (
	"fmt"
	"github.com/hslam/wal"
	"os"
)

func main() {
	path := "wal"
	os.RemoveAll(path)
	log, err := wal.Open(path, &wal.Options{SegmentEntries: 3})
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


### Author
wal was written by Meng Huang.


