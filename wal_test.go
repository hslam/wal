package wal

import (
	"os"
	"testing"
)

func TestWal(t *testing.T) {
	file := "wal"
	os.RemoveAll(file)
	var log *Log
	var err error
	var data []byte
	log, err = OpenWithOptions(file, &Options{SegmentEntries: 3})
	if err != nil {
		t.Error(err)
	}
	log.Write(1, []byte{0, 0, 1})
	log.FlushAndSync()
	log.Write(2, []byte{0, 0, 2})
	log.FlushAndSync()
	log.Write(3, []byte{0, 0, 3})
	log.FlushAndSync()
	log.Write(4, []byte{0, 0, 4})
	log.Write(5, []byte{0, 0, 5})
	log.Write(6, []byte{0, 0, 6})
	log.FlushAndSync()
	data, err = log.Read(1)
	if err != nil {
		t.Error(err)
	}
	if data[2] != 1 {
		t.Error(data)
	}
	err = log.Clean(2)
	if err != nil {
		t.Error(err)
	}
	_, err = log.Read(1)
	if err != ErrOutOfRange {
		t.Error(err)
	}
	data, err = log.Read(6)
	if err != nil {
		t.Error(err)
	}
	err = log.Truncate(5)
	if err != nil {
		t.Error(err)
	}
	_, err = log.Read(6)
	if err != ErrOutOfRange {
		t.Error(err)
	}
	log.Close()
	os.RemoveAll(file)
}
