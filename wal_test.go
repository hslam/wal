package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWal(t *testing.T) {
	file := "wal"
	os.RemoveAll(file)
	var data []byte
	opts := DefaultOptions()
	opts.SegmentEntries = 3
	log, err := Open(file, opts)
	if err != nil {
		t.Error(err)
	}
	log.Write(1, []byte{0, 0, 1})
	log.FlushAndSync()
	log.Write(2, []byte{0, 0, 2})
	log.Flush()
	log.Sync()
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
	log, err = Open(file, &Options{SegmentEntries: 3})
	if err != nil {
		t.Error(err)
	}
	var index uint64
	index, err = log.FirstIndex()
	if err != nil {
		t.Error(err)
	} else if index != 2 {
		t.Error(index)
	}

	index, err = log.LastIndex()
	if err != nil {
		t.Error(err)
	} else if index != 5 {
		t.Error(index)
	}
	var existed bool
	existed, err = log.IsExist(2)
	if err != nil {
		t.Error(err)
	} else if existed != true {
		t.Error(existed)
	}
	data, err = log.Read(2)
	if err != nil {
		t.Error(err)
	}
	if data[2] != 2 {
		t.Error(data)
	}
	err = log.Reset()
	if err != nil {
		t.Error(err)
	}
	err = log.InitFirstIndex(6)
	if err != nil {
		t.Error(err)
	}
	os.RemoveAll(file)
}

func TestOptions(t *testing.T) {
	var opts = &Options{}
	opts.check()
	opts = DefaultOptions()
	opts.Base = 1
	if err := opts.check(); err != ErrBase {
		t.Error(opts.Base)
	}
	opts.Base = 37
	if err := opts.check(); err != ErrBase {
		t.Error(opts.Base)
	}
}

func TestCleanTruncate(t *testing.T) {
	file := "wal"
	os.RemoveAll(file)
	opts := DefaultOptions()
	opts.SegmentEntries = 3
	log, err := Open(file, opts)
	if err != nil {
		t.Error(err)
	}
	for i := uint64(0); i < 12; i++ {
		log.Write(i, []byte{0, 0, byte(i)})
		log.FlushAndSync()
	}
	func(l *Log, index uint64) error {
		l.mu.Lock()
		defer l.mu.Unlock()
		if index == l.firstIndex {
			return nil
		}
		if err := l.checkIndex(index); err != nil {
			return err
		}
		segIndex := l.searchSegmentIndex(index)
		s := l.segments[segIndex]
		if err = l.loadSegment(s); err != nil {
			return err
		}
		cleanName := filepath.Join(l.path, l.logName(index-1)+cleanSuffix)
		start, _ := s.readIndex(index)
		_, end := s.readIndex(s.len)
		offset := int(start)
		size := int(end - start)
		if err = l.copy(s.logPath, cleanName, offset, size); err != nil {
			return err
		}
		return nil
	}(log, 2)
	func(l *Log, index uint64) error {
		l.mu.Lock()
		defer l.mu.Unlock()
		if index == l.lastIndex {
			return nil
		}
		if err := l.checkIndex(index); err != nil {
			return err
		}
		segIndex := l.searchSegmentIndex(index)
		s := l.segments[segIndex]
		if err = l.loadSegment(s); err != nil {
			return err
		}
		truncateName := filepath.Join(l.path, l.logName(s.offset)+truncateSuffix)
		start, _ := s.readIndex(s.offset + 1)
		_, end := s.readIndex(index)
		offset := int(start)
		size := int(end - start)
		if err = l.copy(s.logPath, truncateName, offset, size); err != nil {
			return err
		}
		return nil
	}(log, 5)
	log.Close()

	log, err = Open(file, &Options{SegmentEntries: 3})
	if err != nil {
		t.Error(err)
	}
	log.Close()
	os.RemoveAll(file)
}

func TestOpen(t *testing.T) {
	file := "wal"
	{
		os.RemoveAll(file)
		_, err := Open(file, &Options{Base: 37})
		if err != ErrBase {
			t.Error(err)
		}
		os.RemoveAll(file)
	}
	{
		os.RemoveAll(file)
		log, err := Open(file, nil)
		if err != nil {
			t.Error(err)
		}
		log.Close()
		os.RemoveAll(file)
	}
}

func TestClose(t *testing.T) {
	file := "wal"
	os.RemoveAll(file)
	log, err := Open(file, nil)
	if err != nil {
		t.Error(err)
	}
	for i := uint64(0); i < 12; i++ {
		log.Write(i, []byte{0, 0, byte(i)})
		log.FlushAndSync()
	}
	err = log.Close()
	if err != nil {
		t.Error(err)
	}
	err = log.Write(12, []byte{0, 0, byte(12)})
	if err != ErrClosed {
		t.Error(err)
	}
	err = log.FlushAndSync()
	if err != ErrClosed {
		t.Error(err)
	}
	err = log.Flush()
	if err != ErrClosed {
		t.Error(err)
	}
	err = log.Sync()
	if err != ErrClosed {
		t.Error(err)
	}
	err = log.Clean(2)
	if err != ErrClosed {
		t.Error(err)
	}
	err = log.Truncate(5)
	if err != ErrClosed {
		t.Error(err)
	}
	err = log.Reset()
	if err != ErrClosed {
		t.Error(err)
	}
	_, err = log.FirstIndex()
	if err != ErrClosed {
		t.Error(err)
	}
	_, err = log.LastIndex()
	if err != ErrClosed {
		t.Error(err)
	}
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
