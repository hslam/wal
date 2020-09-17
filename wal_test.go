package wal

import (
	"os"
	"testing"
)

func TestWal(t *testing.T) {
	file := "wal"
	os.RemoveAll(file)
	var data []byte
	log, err := Open(file, &Options{SegmentEntries: 3})
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
	_, err = log.Read(6)
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

func BenchmarkWalWrite(b *testing.B) {
	file := "wal"
	os.RemoveAll(file)
	log, err := Open(file, nil)
	if err != nil {
		b.Error(err)
	}
	var index uint64
	for i := 0; i < b.N; i++ {
		index++
		log.Write(index, []byte{0, 0, 1})
		log.FlushAndSync()
	}
	os.RemoveAll(file)
}

func BenchmarkWalWriteNoSync(b *testing.B) {
	file := "wal"
	os.RemoveAll(file)
	log, err := Open(file, nil)
	if err != nil {
		b.Error(err)
	}
	var index uint64
	for i := 0; i < b.N; i++ {
		index++
		err = log.Write(index, []byte{0, 0, 1})
		if err != nil {
			b.Error(err)
		}
		err = log.Flush()
		if err != nil {
			b.Error(err)
		}
	}
	os.RemoveAll(file)
}

func BenchmarkWalRead(b *testing.B) {
	file := "wal"
	os.RemoveAll(file)
	log, err := Open(file, nil)
	if err != nil {
		b.Error(err)
	}
	log.Write(1, []byte{0, 0, 1})
	log.FlushAndSync()
	for i := 0; i < b.N; i++ {
		data, err := log.Read(1)
		if err != nil {
			b.Error(err)
		}
		if data[2] != 1 {
			b.Error(data)
		}
	}
	os.RemoveAll(file)
}
