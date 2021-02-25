// Copyright (c) 2020 Meng Huang (mhboy@outlook.com)
// This package is licensed under a MIT license that can be found in the LICENSE file.

// Package wal implements write-ahead logging.
package wal

import (
	"errors"
	"fmt"
	"github.com/hslam/code"
	"github.com/hslam/mmap"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const (
	// DefaultSegmentSize is the default segment size.
	DefaultSegmentSize = 1024 * 1024 * 512
	// DefaultSegmentEntries is the default segment entries.
	DefaultSegmentEntries = 1024 * 1024 * 8
	// DefaultWriteBufferSize is the default write buffer size.
	DefaultWriteBufferSize = 1024 * 1024
	// DefaultEncodeBufferSize is the default encode buffer size.
	DefaultEncodeBufferSize = 1024 * 64
	// DefaultBase is the default base.
	DefaultBase = 10
)

const (
	// DefaultLogSuffix is the default log suffix.
	DefaultLogSuffix = ".log"
	// DefaultIndexSuffix is the default index suffix.
	DefaultIndexSuffix = ".idx"
	cleanSuffix        = ".clean"
	truncateSuffix     = ".trunc"
	tmpfile            = "wal.tmp"
)

var (
	// ErrClosed is returned when the log is closed.
	ErrClosed = errors.New("closed")
	// ErrUnexpectedSize is returned when the number of bytes is unexpected.
	ErrUnexpectedSize = errors.New("unexpected size")
	// ErrOutOfRange is returned when the index is out of range.
	ErrOutOfRange = errors.New("out of range")
	// ErrZeroIndex is returned because the index must be greater than zero.
	ErrZeroIndex = errors.New("index can not be zero")
	// ErrOutOfOrder is returned when the index is out of order. The index must be equal to LastIndex + 1
	ErrOutOfOrder = errors.New("out of order")
	// ErrBase is returned when base < 2 or base > 36
	ErrBase = errors.New("2 <= base <= 36")
)

// WAL represents a write-ahead log.
type WAL struct {
	mu             sync.Mutex
	wg             sync.WaitGroup
	path           string
	segmentSize    int
	segmentEntries int
	indexSpace     int
	logSuffix      string
	indexSuffix    string
	base           int
	noSplitSegment bool
	nameLength     int
	closed         bool
	segments       []*segment
	firstIndex     uint64
	lastIndex      uint64
	lastSegment    *segment
	encodeBuffer   []byte
	writeBuffer    []byte
}

type segment struct {
	logPath     string
	indexPath   string
	indexSpace  int
	offset      uint64
	len         uint64
	indexFile   *os.File
	indexMmap   []byte
	logFile     *os.File
	indexBuffer []byte
}

func (s *segment) readIndex(index uint64) (start, end uint64) {
	r := index - s.offset
	if r == 1 {
		start = 0
		copy(s.indexBuffer, s.indexMmap[8:16])
		code.DecodeUint64(s.indexBuffer, &end)
	} else {
		copy(s.indexBuffer, s.indexMmap[r*8-8:r*8])
		code.DecodeUint64(s.indexBuffer, &start)
		copy(s.indexBuffer, s.indexMmap[r*8:r*8+8])
		code.DecodeUint64(s.indexBuffer, &end)
	}
	return
}

func (s *segment) load() error {
	var err error
	if s.indexFile == nil {
		if s.indexFile, err = os.Create(s.indexPath); err != nil {
			return err
		}
		if mmap.Fsize(s.indexFile) != s.indexSpace {
			if err = s.indexFile.Truncate(int64(s.indexSpace)); err != nil {
				return err
			}
		}
		if s.indexMmap, err = mmap.Open(mmap.Fd(s.indexFile), 0, mmap.Fsize(s.indexFile), mmap.READ|mmap.WRITE); err != nil {
			return err
		}
	}
	copy(s.indexBuffer, s.indexMmap[:8])
	code.DecodeUint64(s.indexBuffer, &s.len)
	copy(s.indexBuffer, s.indexMmap[s.len*8:s.len*8+8])
	var size uint64
	code.DecodeUint64(s.indexBuffer, &size)
	if s.logFile == nil {
		if s.logFile, err = os.Open(s.logPath); err != nil {
			return err
		}
	}
	if int(size) != mmap.Fsize(s.logFile) {
		m, err := mmap.Open(mmap.Fd(s.logFile), 0, mmap.Fsize(s.logFile), mmap.READ)
		if err != nil {
			return err
		}
		defer mmap.Munmap(m)
		data := m[:]
		var position, i int
		for i = 1; len(data) > 0; i++ {
			var n int
			var size uint64
			n = int(code.DecodeVarint(data, &size))
			n += int(size)
			data = data[n:]
			code.EncodeUint64(s.indexBuffer, uint64(position+n))
			copy(s.indexMmap[i*8:i*8+8], s.indexBuffer)
			position += n
		}
		code.EncodeUint64(s.indexBuffer, uint64(i-1))
		copy(s.indexMmap[:8], s.indexBuffer)
		s.len = uint64(i - 1)
	}
	return nil
}

func (s *segment) remove() (err error) {
	os.Remove(s.indexPath)
	return os.Remove(s.logPath)
}

func (s *segment) close() (err error) {
	if s.logFile != nil {
		if err = s.logFile.Sync(); err != nil {
			return err
		}
		if err = s.logFile.Close(); err != nil {
			return err
		}
		s.logFile = nil
	}
	if s.indexFile != nil {
		if err = s.indexFile.Close(); err != nil {
			return err
		}
		s.indexFile = nil
	}
	if len(s.indexMmap) > 0 {
		err = mmap.Munmap(s.indexMmap)
		s.indexMmap = []byte{}
	}
	s.len = 0
	return err
}

// Options represents options
type Options struct {
	// SegmentSize is the segment size.
	SegmentSize int
	// SegmentEntries is the number of segment entries.
	SegmentEntries int
	// EncodeBufferSize is the encode buffer size.
	EncodeBufferSize int
	// WriteBufferSize is the write buffer size.
	WriteBufferSize int
	// LogSuffix is the log suffix.
	LogSuffix string
	// IndexSuffix is the index suffix.
	IndexSuffix string
	// Base is the base.
	Base int
	// NoSplitSegment is used by the Clean method. When this option is set,
	// do not split the segment. Default is false .
	NoSplitSegment bool
}

// DefaultOptions returns default options.
func DefaultOptions() *Options {
	return &Options{
		SegmentSize:      DefaultSegmentSize,
		SegmentEntries:   DefaultSegmentEntries,
		EncodeBufferSize: DefaultEncodeBufferSize,
		WriteBufferSize:  DefaultWriteBufferSize,
		LogSuffix:        DefaultLogSuffix,
		IndexSuffix:      DefaultIndexSuffix,
		Base:             DefaultBase,
	}
}

func (opts *Options) check() error {
	if opts.SegmentSize < 1 {
		opts.SegmentSize = DefaultSegmentSize
	}
	if opts.SegmentEntries < 1 {
		opts.SegmentEntries = DefaultSegmentEntries
	}
	if opts.EncodeBufferSize < 1 {
		opts.EncodeBufferSize = DefaultEncodeBufferSize
	}
	if opts.WriteBufferSize < 1 {
		opts.WriteBufferSize = DefaultWriteBufferSize
	}
	if len(opts.LogSuffix) < 1 {
		opts.LogSuffix = DefaultLogSuffix
	}
	if len(opts.IndexSuffix) < 1 {
		opts.IndexSuffix = DefaultIndexSuffix
	}
	if opts.Base < 1 {
		opts.Base = DefaultBase
	} else if opts.Base < 2 || opts.Base > 36 {
		return ErrBase
	}
	return nil
}

// Open opens a write-ahead log with options.
func Open(path string, opts *Options) (w *WAL, err error) {
	if opts != nil {
		err = opts.check()
		if err != nil {
			return
		}
	} else {
		opts = DefaultOptions()
	}
	w = &WAL{
		path:           path,
		segmentSize:    opts.SegmentSize,
		segmentEntries: opts.SegmentEntries,
		indexSpace:     opts.SegmentEntries*8 + 8,
		logSuffix:      opts.LogSuffix,
		indexSuffix:    opts.IndexSuffix,
		base:           opts.Base,
		noSplitSegment: opts.NoSplitSegment,
		nameLength:     len(strconv.FormatUint(1<<64-1, opts.Base)),
		encodeBuffer:   make([]byte, opts.EncodeBufferSize),
		writeBuffer:    make([]byte, 0, opts.WriteBufferSize),
	}
	err = w.load()
	if err != nil {
		w = nil
	}
	return
}

func (w *WAL) load() (err error) {
	err = os.MkdirAll(w.path, 0744)
	if err != nil {
		return
	}
	tmpName := filepath.Join(w.path, tmpfile)
	_, err = os.Stat(tmpName)
	if !os.IsNotExist(err) {
		os.Remove(tmpName)
	}
	truncate := false
	err = filepath.Walk(w.path, func(filePath string, info os.FileInfo, err error) error {
		name, n := info.Name(), w.nameLength
		if len(name) < n+len(w.logSuffix) || info.IsDir() {
			return nil
		}
		if name[n:n+len(w.logSuffix)] != w.logSuffix {
			return nil
		}
		offset, err := w.parseSegmentName(name[:n])
		if err != nil {
			return nil
		}
		if len(name) == n+len(w.logSuffix) {
			if truncate {
				if err := os.Remove(filePath); err != nil {
					return err
				}
				if err := os.Remove(filepath.Join(w.path, name[:n]+w.indexSuffix)); err != nil {
					return err
				}
				return nil
			}
		} else {
			if len(name) == n+len(w.logSuffix)+len(cleanSuffix) && strings.HasSuffix(name, cleanSuffix) {
				for i := 0; i < len(w.segments); i++ {
					w.segments[i].remove()
				}
				w.segments = []*segment{}
				if err := os.Rename(filePath, filepath.Join(w.path, name[:n+len(w.logSuffix)])); err != nil {
					return err
				}
			} else if len(name) == n+len(w.logSuffix)+len(truncateSuffix) && strings.HasSuffix(name, truncateSuffix) {
				truncate = true
				if len(w.segments) > 0 && w.segments[len(w.segments)-1].offset == offset {
					w.segments[len(w.segments)-1].remove()
					w.segments = w.segments[:len(w.segments)-1]
				}
				if err := os.Rename(filePath, filepath.Join(w.path, name[:n+len(w.logSuffix)])); err != nil {
					return err
				}
			}
			name = name[:n+len(w.logSuffix)]
		}
		w.segments = append(w.segments, &segment{
			offset:      offset,
			logPath:     filepath.Join(w.path, name),
			indexPath:   filepath.Join(w.path, name[:n]+w.indexSuffix),
			indexBuffer: make([]byte, 8),
			indexSpace:  w.indexSpace,
		})
		return nil
	})
	if err != nil {
		return err
	}
	if len(w.segments) > 0 {
		w.firstIndex = w.segments[0].offset + 1
		return w.resetLastSegment()
	}
	w.firstIndex = 1
	return nil
}

func (w *WAL) appendSegment() (err error) {
	if err = w.closeLastSegment(); err != nil {
		return err
	}
	s := &segment{
		offset:      w.lastIndex,
		logPath:     filepath.Join(w.path, w.logName(w.lastIndex)),
		indexPath:   filepath.Join(w.path, w.indexName(w.lastIndex)),
		indexBuffer: make([]byte, 8),
		indexSpace:  w.indexSpace,
	}
	w.segments = append(w.segments, s)
	w.lastSegment = s
	if s.logFile, err = os.Create(s.logPath); err != nil {
		return err
	}
	if s.indexFile, err = os.Create(s.indexPath); err != nil {
		return err
	}
	if err = s.indexFile.Truncate(int64(w.indexSpace)); err != nil {
		return err
	}
	if err = s.indexFile.Sync(); err != nil {
		return err
	}
	if s.indexMmap, err = mmap.Open(mmap.Fd(s.indexFile), 0, mmap.Fsize(s.indexFile), mmap.READ|mmap.WRITE); err != nil {
		return err
	}
	return
}

func (w *WAL) resetLastSegment() (err error) {
	if err = w.closeLastSegment(); err != nil {
		return err
	}
	lastSegment := w.segments[len(w.segments)-1]
	w.lastSegment = lastSegment
	if lastSegment.logFile, err = os.OpenFile(lastSegment.logPath, os.O_RDWR, 0666); err != nil {
		return err
	}
	if n, err := lastSegment.logFile.Seek(0, os.SEEK_END); err != nil {
		return err
	} else if n <= 0 {
		w.lastIndex = lastSegment.offset
		return nil
	}
	if err := lastSegment.load(); err != nil {
		return err
	}
	w.lastIndex = lastSegment.offset + uint64(lastSegment.len)
	return nil
}

func (w *WAL) closeLastSegment() (err error) {
	if w.lastSegment != nil {
		err = w.lastSegment.close()
	}
	return err
}

func (w *WAL) loadSegment(s *segment) (err error) {
	if s.len == 0 {
		if err := s.load(); err != nil {
			return err
		}
	}
	return nil
}

// Reset discards all entries.
func (w *WAL) Reset() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.reset()
}

func (w *WAL) reset() (err error) {
	if err = w.empty(); err != nil {
		return err
	}
	w.firstIndex = 1
	w.lastIndex = 0
	w.lastSegment = nil
	w.segments = w.segments[:0]
	return nil
}

func (w *WAL) empty() (err error) {
	if w.closed {
		return ErrClosed
	}
	if err = w.close(); err != nil {
		return err
	}
	err = filepath.Walk(w.path, func(filePath string, info os.FileInfo, err error) error {
		if info == nil || err != nil {
			return nil
		}
		name, n := info.Name(), w.nameLength
		if len(name) < n || info.IsDir() {
			return nil
		}
		_, err = w.parseSegmentName(name[:n])
		if err != nil {
			return nil
		}
		if name[n:n+len(w.logSuffix)] != w.logSuffix && name[n:n+len(w.indexSuffix)] != w.indexSuffix {
			return nil
		}
		if err := os.Remove(filePath); err != nil {
			return err
		}
		return nil
	})
	return err
}

// Write writes an entry to buffer.
func (w *WAL) Write(index uint64, data []byte) (err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if index == 0 {
		return ErrZeroIndex
	}
	if w.lastIndex > 0 && index != w.lastIndex+1 {
		return ErrOutOfOrder
	} else if w.lastIndex == 0 {
		w.firstIndex = index
		w.lastIndex = index - 1
	}
	if len(w.segments) == 0 {
		if err = w.appendSegment(); err != nil {
			return err
		}
	}
	end, err := w.lastSegment.logFile.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}
	offset := int(end)
	size := 10 + len(data)
	if cap(w.encodeBuffer) >= size {
		w.encodeBuffer = w.encodeBuffer[:size]
	} else {
		w.encodeBuffer = make([]byte, size)
	}
	n := code.EncodeVarint(w.encodeBuffer, uint64(len(data)))
	copy(w.encodeBuffer[n:], data)
	entryData := w.encodeBuffer[:int(n)+len(data)]
	if offset+len(w.writeBuffer)+len(entryData) > w.segmentSize || int(index-w.lastSegment.offset) > w.segmentEntries {
		if err := w.flush(); err != nil {
			return err
		}
		if err := w.sync(); err != nil {
			return err
		}
		if err := w.appendSegment(); err != nil {
			return err
		}
		w.lastSegment = w.segments[len(w.segments)-1]
		offset = 0
	}
	entries := index - w.lastSegment.offset
	code.EncodeUint64(w.lastSegment.indexBuffer, uint64(entries))
	copy(w.lastSegment.indexMmap, w.lastSegment.indexBuffer)
	code.EncodeUint64(w.lastSegment.indexBuffer, uint64(offset+len(w.writeBuffer)+len(entryData)))
	copy(w.lastSegment.indexMmap[entries*8:entries*8+8], w.lastSegment.indexBuffer)
	w.lastSegment.len = entries
	w.writeBuffer = append(w.writeBuffer, entryData...)
	w.lastIndex = index
	return nil
}

// Flush writes buffered data to file.
func (w *WAL) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flush()
}

func (w *WAL) flush() error {
	if w.closed {
		return ErrClosed
	}
	if len(w.writeBuffer) > 0 {
		if _, err := w.lastSegment.logFile.Write(w.writeBuffer); err != nil {
			return err
		}
		w.writeBuffer = w.writeBuffer[:0]
	}
	return nil
}

// Sync commits the current contents of the file to stable storage.
// Typically, this means flushing the file system's in-memory copy
// of recently written data to disk.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sync()
}

func (w *WAL) sync() error {
	if w.closed {
		return ErrClosed
	}
	if w.lastSegment != nil {
		if err := w.lastSegment.logFile.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// Close closes the write-ahead log.
func (w *WAL) Close() (err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err = w.flush(); err != nil {
		return err
	}
	if err = w.sync(); err != nil {
		return err
	}
	if w.closed {
		return nil
	}
	w.closed = true
	return w.close()
}

func (w *WAL) close() (err error) {
	for i := 0; i < len(w.segments); i++ {
		if err = w.segments[i].close(); err != nil {
			return err
		}
	}
	w.wg.Wait()
	return
}

// FirstIndex returns the write-ahead log first index.
func (w *WAL) FirstIndex() (index uint64, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, ErrClosed
	}
	return w.firstIndex, nil
}

// LastIndex returns the write-ahead log last index.
func (w *WAL) LastIndex() (index uint64, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, ErrClosed
	}
	return w.lastIndex, nil
}

func (w *WAL) searchSegmentIndex(index uint64) int {
	low := 0
	high := len(w.segments) - 1
	for low <= high {
		mid := (low + high) / 2
		if index > w.segments[mid].offset {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return high
}

// IsExist returns true when the index is in range.
func (w *WAL) IsExist(index uint64) (bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.checkIndex(index); err != nil {
		if err == ErrClosed {
			return false, err
		}
		return false, nil
	}
	return true, nil
}

func (w *WAL) checkIndex(index uint64) error {
	if w.closed {
		return ErrClosed
	}
	if index == 0 || w.lastIndex == 0 || index < w.firstIndex || index > w.lastIndex {
		return ErrOutOfRange
	}
	return nil
}

// Read returns an entry by index.
func (w *WAL) Read(index uint64) (data []byte, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.checkIndex(index); err != nil {
		return nil, err
	}
	segIndex := w.searchSegmentIndex(index)
	s := w.segments[segIndex]
	if err = w.loadSegment(s); err != nil {
		return nil, err
	}
	var start, end = s.readIndex(index)
	ret, _ := s.logFile.Seek(int64(start), os.SEEK_SET)
	entryData := make([]byte, end-start)
	n, err := s.logFile.ReadAt(entryData, ret)
	if err != nil {
		return nil, err
	}
	if len(entryData) != n {
		return nil, ErrUnexpectedSize
	}
	var size uint64
	n = int(code.DecodeVarint(entryData, &size))
	if uint64(len(entryData)-n) != size {
		return nil, ErrUnexpectedSize
	}
	return entryData[n:], nil
}

// Clean cleans up the old entries before index.
func (w *WAL) Clean(index uint64) (err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
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
	if s.offset == index-1 {
		for i := 0; i < segIndex; i++ {
			w.segments[i].close()
			w.segments[i].remove()
		}
		w.segments = w.segments[segIndex:]
		w.firstIndex = index
		return nil
	}
	if w.noSplitSegment {
		if segIndex > 0 {
			removes := w.segments[:segIndex]
			w.segments = w.segments[segIndex:]
			w.firstIndex = w.segments[0].offset + 1
			w.wg.Add(1)
			go func(removes []*segment) {
				for i := 0; i < len(removes); i++ {
					removes[i].close()
					removes[i].remove()
				}
				w.wg.Done()
			}(removes)
		}
		return
	}
	cleanName := filepath.Join(w.path, w.logName(index-1)+cleanSuffix)
	start, _ := s.readIndex(index)
	_, end := s.readIndex(s.offset + s.len)
	offset := int(start)
	size := int(end - start)
	if err = w.copy(s.logPath, cleanName, offset, size); err != nil {
		return err
	}
	for i := 0; i <= segIndex; i++ {
		w.segments[i].close()
		w.segments[i].remove()
	}
	name := filepath.Join(w.path, w.logName(index-1))
	if err = os.Rename(cleanName, name); err != nil {
		return err
	}
	s.logPath = name
	s.indexPath = filepath.Join(w.path, w.indexName(index-1))
	s.offset = index - 1
	s.len = 0
	w.segments = w.segments[segIndex:]
	w.firstIndex = index
	if len(w.segments) == 1 {
		return w.resetLastSegment()
	}
	return nil
}

// Truncate deletes the dirty entries after index.
func (w *WAL) Truncate(index uint64) (err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
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
	if len(w.segments) > segIndex+1 {
		next := w.segments[segIndex+1]
		if err = w.loadSegment(next); err != nil {
			return err
		}
		if next.offset == index {
			for i := segIndex + 1; i < len(w.segments); i++ {
				w.segments[i].close()
				w.segments[i].remove()
			}
			w.segments = w.segments[:segIndex+1]
			w.lastIndex = index
			return
		}
	}
	truncateName := filepath.Join(w.path, w.logName(s.offset)+truncateSuffix)
	start, _ := s.readIndex(s.offset + 1)
	_, end := s.readIndex(index)
	offset := int(start)
	size := int(end - start)
	if err = w.copy(s.logPath, truncateName, offset, size); err != nil {
		return err
	}
	for i := segIndex; i < len(w.segments); i++ {
		w.segments[i].close()
		w.segments[i].remove()
	}
	filePath := filepath.Join(w.path, w.logName(s.offset))
	if err = os.Rename(truncateName, filePath); err != nil {
		return err
	}
	s.logPath = filePath
	w.segments = w.segments[:segIndex+1]
	w.lastIndex = index
	return w.resetLastSegment()
}

func (w *WAL) copy(srcName string, dstName string, offset, size int) (err error) {
	var srcFile, tmpFile *os.File
	if srcFile, err = os.Open(srcName); err != nil {
		return err
	}
	var m []byte
	if m, err = mmap.Open(mmap.Fd(srcFile), 0, mmap.Fsize(srcFile), mmap.READ); err != nil {
		return err
	}
	tmpName := filepath.Join(w.path, tmpfile)
	if tmpFile, err = os.Create(tmpName); err != nil {
		return err
	}
	if err = tmpFile.Truncate(int64(size)); err != nil {
		return err
	}
	if err = tmpFile.Sync(); err != nil {
		return err
	}
	var tmpMmap []byte
	if tmpMmap, err = mmap.Open(mmap.Fd(tmpFile), 0, mmap.Fsize(tmpFile), mmap.WRITE); err != nil {
		return err
	}
	copy(tmpMmap, m[offset:offset+size])
	if err = mmap.Msync(tmpMmap); err != nil {
		return err
	}
	if err = mmap.Munmap(tmpMmap); err != nil {
		return err
	}
	if err = tmpFile.Sync(); err != nil {
		return err
	}
	if err = tmpFile.Close(); err != nil {
		return err
	}
	if err = mmap.Munmap(m); err != nil {
		return err
	}
	if err = srcFile.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmpName, dstName); err != nil {
		return err
	}
	return nil
}

func (w *WAL) logName(offset uint64) string {
	return w.segmentName(offset) + w.logSuffix
}

func (w *WAL) indexName(offset uint64) string {
	return w.segmentName(offset) + w.indexSuffix
}

func (w *WAL) segmentName(offset uint64) string {
	return fmt.Sprintf("%0"+fmt.Sprintf("%d", w.nameLength)+"s", strconv.FormatUint(offset, w.base))
}

func (w *WAL) parseSegmentName(segmentName string) (uint64, error) {
	offset, err := strconv.ParseUint(segmentName[:w.nameLength], w.base, 64)
	if err != nil {
		return 0, err
	}
	return offset, nil
}
