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
	w, err := Open(file, opts)
	if err != nil {
		t.Error(err)
	}
	w.Write(1, []byte{0, 0, 1})
	w.Flush()
	w.Sync()
	w.Write(2, []byte{0, 0, 2})
	w.Flush()
	w.Sync()
	w.Write(3, []byte{0, 0, 3})
	w.Flush()
	w.Sync()
	w.Write(4, []byte{0, 0, 4})
	w.Write(5, []byte{0, 0, 5})
	w.Write(6, []byte{0, 0, 6})
	w.Flush()
	w.Sync()
	data, err = w.Read(1)
	if err != nil {
		t.Error(err)
	}
	if data[2] != 1 {
		t.Error(data)
	}
	err = w.Clean(2)
	if err != nil {
		t.Error(err)
	}
	_, err = w.Read(1)
	if err != ErrOutOfRange {
		t.Error(err)
	}
	_, err = w.Read(6)
	if err != nil {
		t.Error(err)
	}
	err = w.Truncate(5)
	if err != nil {
		t.Error(err)
	}
	_, err = w.Read(6)
	if err != ErrOutOfRange {
		t.Error(err)
	}
	if ok, _ := w.IsExist(6); ok {
		t.Error()
	}
	w.Close()
	w, err = Open(file, &Options{SegmentEntries: 3})
	if err != nil {
		t.Error(err)
	}
	var index uint64
	index, err = w.FirstIndex()
	if err != nil {
		t.Error(err)
	} else if index != 2 {
		t.Error(index)
	}

	index, err = w.LastIndex()
	if err != nil {
		t.Error(err)
	} else if index != 5 {
		t.Error(index)
	}
	var existed bool
	existed, err = w.IsExist(2)
	if err != nil {
		t.Error(err)
	} else if existed != true {
		t.Error(existed)
	}
	data, err = w.Read(2)
	if err != nil {
		t.Error(err)
	}
	if data[2] != 2 {
		t.Error(data)
	}
	err = w.Reset()
	if err != nil {
		t.Error(err)
	}
	w.Close()
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
	w, err := Open(file, opts)
	if err != nil {
		t.Error(err)
	}
	for i := uint64(0); i < 12; i++ {
		w.Write(i, []byte{0, 0, byte(i)})
		w.Flush()
		w.Sync()
	}
	func(w *WAL, index uint64) error {
		if index == w.firstIndex {
			return nil
		}
		if err := w.checkIndex(index); err != nil {
			return err
		}
		segIndex := w.searchSegmentIndex(index)
		s := w.segments[segIndex]
		if err = w.loadSegment(s); err != nil {
			return err
		}
		cleanName := filepath.Join(w.path, w.logName(index-1)+cleanSuffix)
		start, _ := s.readIndex(index)
		_, end := s.readIndex(s.len)
		offset := int(start)
		size := int(end - start)
		if err = w.copy(s.logPath, cleanName, offset, size); err != nil {
			return err
		}
		return nil
	}(w, 2)
	func(w *WAL, index uint64) error {
		if index == w.lastIndex {
			return nil
		}
		if err := w.checkIndex(index); err != nil {
			return err
		}
		segIndex := w.searchSegmentIndex(index)
		s := w.segments[segIndex]
		if err = w.loadSegment(s); err != nil {
			return err
		}
		truncateName := filepath.Join(w.path, w.logName(s.offset)+truncateSuffix)
		start, _ := s.readIndex(s.offset + 1)
		_, end := s.readIndex(index)
		offset := int(start)
		size := int(end - start)
		if err = w.copy(s.logPath, truncateName, offset, size); err != nil {
			return err
		}
		return nil
	}(w, 5)
	w.Close()

	w, err = Open(file, &Options{SegmentEntries: 3})
	if err != nil {
		t.Error(err)
	}
	w.Close()
	os.RemoveAll(file)
}

func TestCleanTruncateMore(t *testing.T) {
	file := "wal"
	os.RemoveAll(file)
	w, err := Open(file, &Options{SegmentEntries: 3})
	if err != nil {
		t.Error(err)
	}
	for i := uint64(0); i < 12; i++ {
		w.Write(i, []byte{0, 0, byte(i)})
		w.Flush()
		w.Sync()
	}
	w.Clean(4)
	w.Truncate(6)
	w.Clean(5)
	w.Truncate(5)
	w.Close()
	os.RemoveAll(file)
}

func TestNoSplitSegment(t *testing.T) {
	file := "wal"
	os.RemoveAll(file)
	w, err := Open(file, &Options{SegmentEntries: 3, NoSplitSegment: true})
	if err != nil {
		t.Error(err)
	}
	for i := uint64(0); i < 12; i++ {
		w.Write(i, []byte{0, 0, byte(i)})
		w.Flush()
		w.Sync()
	}
	w.Clean(8)
	_, err = w.Read(1)
	if err != ErrOutOfRange {
		t.Error(err)
	}
	w.Close()
	os.RemoveAll(file)
}

func TestParseSegmentName(t *testing.T) {
	file := "wal"
	os.RemoveAll(file)
	w, err := Open(file, DefaultOptions())
	if err != nil {
		t.Error(err)
	}
	if v, err := w.parseSegmentName("00000000000000000001"); v != 1 || err != nil {
		t.Error()
	} else if _, err := w.parseSegmentName("0000000000000000000i"); err == nil {
		t.Error()
	}
	os.RemoveAll(file)
}

func TestTmpFile(t *testing.T) {
	file := "wal"
	os.RemoveAll(file)
	w, err := Open(file, DefaultOptions())
	if err != nil {
		t.Error(err)
	}
	w.Close()
	tmpName := filepath.Join(w.path, tmpfile)
	if tmpFile, err := os.Create(tmpName); err == nil {
		tmpFile.Close()
	}
	w, err = Open(file, DefaultOptions())
	if err != nil {
		t.Error(err)
	}
	w.Close()
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
		w, err := Open(file, nil)
		if err != nil {
			t.Error(err)
		}
		w.Close()
		os.RemoveAll(file)
	}
}

func TestClose(t *testing.T) {
	file := "wal"
	os.RemoveAll(file)
	w, err := Open(file, nil)
	if err != nil {
		t.Error(err)
	}
	for i := uint64(0); i < 12; i++ {
		w.Write(i, []byte{0, 0, byte(i)})
		w.Flush()
		w.Sync()
	}
	err = w.Close()
	if err != nil {
		t.Error(err)
	}
	err = w.Write(12, []byte{0, 0, byte(12)})
	if err != ErrClosed {
		t.Error(err)
	}
	err = w.Flush()
	if err != ErrClosed {
		t.Error(err)
	}
	err = w.Sync()
	if err != ErrClosed {
		t.Error(err)
	}
	err = w.Clean(2)
	if err != ErrClosed {
		t.Error(err)
	}
	err = w.Truncate(5)
	if err != ErrClosed {
		t.Error(err)
	}
	err = w.Reset()
	if err != ErrClosed {
		t.Error(err)
	}
	_, err = w.FirstIndex()
	if err != ErrClosed {
		t.Error(err)
	}
	_, err = w.LastIndex()
	if err != ErrClosed {
		t.Error(err)
	}
	_, err = w.IsExist(3)
	if err != ErrClosed {
		t.Error(err)
	}
	w.Close()
	os.RemoveAll(file)
}

func BenchmarkWalWrite(b *testing.B) {
	file := "wal"
	os.RemoveAll(file)
	w, err := Open(file, nil)
	if err != nil {
		b.Error(err)
	}
	var index uint64
	for i := 0; i < b.N; i++ {
		index++
		w.Write(index, []byte{0, 0, 1})
		w.Flush()
		w.Sync()
	}
	os.RemoveAll(file)
}

func BenchmarkWalWriteNoSync(b *testing.B) {
	file := "wal"
	os.RemoveAll(file)
	w, err := Open(file, nil)
	if err != nil {
		b.Error(err)
	}
	var index uint64
	for i := 0; i < b.N; i++ {
		index++
		err = w.Write(index, []byte{0, 0, 1})
		if err != nil {
			b.Error(err)
		}
		err = w.Flush()
		if err != nil {
			b.Error(err)
		}
	}
	os.RemoveAll(file)
}

func BenchmarkWalRead(b *testing.B) {
	file := "wal"
	os.RemoveAll(file)
	w, err := Open(file, nil)
	if err != nil {
		b.Error(err)
	}
	w.Write(1, []byte{0, 0, 1})
	w.Flush()
	w.Sync()
	for i := 0; i < b.N; i++ {
		data, err := w.Read(1)
		if err != nil {
			b.Error(err)
		}
		if data[2] != 1 {
			b.Error(data)
		}
	}
	os.RemoveAll(file)
}
