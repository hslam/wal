# wal
Write-Ahead Logging

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
	file := "wal"
	os.RemoveAll(file)
	var log *wal.Log
	var err error
	var data []byte
	log, err = wal.OpenWithOptions(file, &wal.Options{SegmentEntries: 3})
	if err != nil {
		panic(err)
	}
	log.Write(1, []byte{0, 0, 1})
	log.Flush()
	log.Write(2, []byte{0, 0, 2})
	log.Flush()
	log.Write(3, []byte{0, 0, 3})
	log.Flush()
	log.Write(4, []byte{0, 0, 4})
	log.Write(5, []byte{0, 0, 5})
	log.Write(6, []byte{0, 0, 6})
	log.Flush()
	data, _ = log.Read(1)
	fmt.Println(data)
	log.Clean(2)
	fmt.Println("Clean", 2)
	_, err = log.Read(1)
	fmt.Println(1, err)
	data, _ = log.Read(6)
	fmt.Println(data)
	log.Truncate(5)
	fmt.Println("Truncate", 5)
	_, err = log.Read(6)
	fmt.Println(6, err)
	log.Close()

	log, err = wal.OpenWithOptions(file, &wal.Options{SegmentEntries: 3})
	if err != nil {
		panic(err)
	}
	data, _ = log.Read(2)
	fmt.Println(data)
	data, _ = log.Read(5)
	fmt.Println(data)
	log.Close()
}
```

### Output
```
[0 0 1]
Clean 2
1 out of range
[0 0 6]
Truncate 5
6 out of range
[0 0 2]
[0 0 5]
```

### License
This package is licensed under a MIT license (Copyright (c) 2020 Meng Huang)


### Authors
wal was written by Meng Huang.


