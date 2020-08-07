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

var (
	ErrClosed      = errors.New("closed")
	ErrShortBuffer = errors.New("short buffer")
	ErrOutOfRange  = errors.New("out of range")
	ErrZeroIndex   = errors.New("index can not be zero")
	ErrOutOfOrder  = errors.New("out of order")
)

const (
	DefaultSegmentSize    = 1024 * 1024 * 512
	DefaultSegmentEntries = 1024 * 1024 * 8
	DefaultWriteBuffer    = 1024 * 1024
	DefaultEncodeBuffer   = 1024 * 64
	DefaultBase           = 10
)

const (
	DefaultLogSuffix   = ".log"
	DefaultIndexSuffix = ".idx"
	cleanSuffix        = ".clean"
	truncateSuffix     = ".trunc"
)

type Log struct {
	mu             sync.Mutex
	path           string
	segmentSize    int
	segmentEntries int
	indexSpace     int
	logSuffix      string
	indexSuffix    string
	base           int
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
		if s.indexMmap, err = mmap.Open(mmap.Fd(s.indexFile), mmap.Fsize(s.indexFile), mmap.READ|mmap.WRITE); err != nil {
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
		m, err := mmap.Open(mmap.Fd(s.logFile), mmap.Fsize(s.logFile), mmap.READ)
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

type Options struct {
	SegmentSize    int
	SegmentEntries int
	EncodeBuffer   int
	WriteBuffer    int
	LogSuffix      string
	IndexSuffix    string
	Base           int
}

func DefaultOptions() *Options {
	return &Options{
		SegmentSize:    DefaultSegmentSize,
		SegmentEntries: DefaultSegmentEntries,
		EncodeBuffer:   DefaultEncodeBuffer,
		WriteBuffer:    DefaultWriteBuffer,
		LogSuffix:      DefaultLogSuffix,
		IndexSuffix:    DefaultIndexSuffix,
		Base:           DefaultBase,
	}
}

func (opts *Options) check() error {
	if opts.SegmentSize < 1 {
		opts.SegmentSize = DefaultSegmentSize
	}
	if opts.SegmentEntries < 1 {
		opts.SegmentEntries = DefaultSegmentEntries
	}
	if opts.EncodeBuffer < 1 {
		opts.EncodeBuffer = DefaultEncodeBuffer
	}
	if opts.WriteBuffer < 1 {
		opts.WriteBuffer = DefaultWriteBuffer
	}
	if len(opts.LogSuffix) < 1 {
		opts.LogSuffix = DefaultLogSuffix
	}
	if len(opts.IndexSuffix) < 1 {
		opts.IndexSuffix = DefaultIndexSuffix
	}
	if opts.Base < 1 {
		opts.Base = DefaultBase
	} else {
		if opts.Base < 2 {
			opts.Base = 2
		} else if opts.Base > 36 {
			opts.Base = 36
		}
	}
	return nil
}

func Open(path string) (*Log, error) {
	return OpenWithOptions(path, DefaultOptions())
}

func OpenWithOptions(path string, opts *Options) (l *Log, err error) {
	if opts == nil {
		opts = DefaultOptions()
	} else {
		if err = opts.check(); err != nil {
			return nil, err
		}
	}
	l = &Log{
		path:           path,
		segmentSize:    opts.SegmentSize,
		segmentEntries: opts.SegmentEntries,
		indexSpace:     opts.SegmentEntries*8 + 8,
		logSuffix:      opts.LogSuffix,
		indexSuffix:    opts.IndexSuffix,
		base:           opts.Base,
		nameLength:     len(strconv.FormatUint(1<<64-1, opts.Base)),
		encodeBuffer:   make([]byte, opts.EncodeBuffer),
		writeBuffer:    make([]byte, 0, opts.WriteBuffer),
	}
	if err = l.load(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *Log) load() error {
	if err := os.MkdirAll(l.path, 0777); err != nil {
		return err
	}
	truncate := false
	err := filepath.Walk(l.path, func(filePath string, info os.FileInfo, err error) error {
		name, n := info.Name(), l.nameLength
		if len(name) < n+len(l.logSuffix) || info.IsDir() {
			return nil
		}
		if name[n:n+len(l.logSuffix)] != l.logSuffix {
			return nil
		}
		offset, err := l.parseSegmentName(name[:n])
		if err != nil {
			return nil
		}
		if len(name) == n+len(l.logSuffix) {
			if truncate {
				if err := os.Remove(filePath); err != nil {
					return err
				}
				if err := os.Remove(filepath.Join(l.path, name[:n]+l.indexSuffix)); err != nil {
					return err
				}
				return nil
			}
		} else {
			if len(name) == n+len(l.logSuffix)+len(cleanSuffix) && strings.HasSuffix(name, cleanSuffix) {
				for i := 0; i < len(l.segments); i++ {
					if err := os.Remove(l.segments[i].logPath); err != nil {
						return err
					}
					if err := os.Remove(l.segments[i].indexPath); err != nil {
						return err
					}
				}
				l.segments = []*segment{}
				if err := os.Rename(filePath, filepath.Join(l.path, name[:n+len(l.logSuffix)])); err != nil {
					return err
				}
			} else if len(name) == n+len(l.logSuffix)+len(truncateSuffix) && strings.HasSuffix(name, truncateSuffix) {
				truncate = true
				if len(l.segments) > 0 && l.segments[len(l.segments)-1].offset == offset {
					if err := os.Remove(l.segments[len(l.segments)-1].logPath); err != nil {
						return err
					}
					if err := os.Remove(l.segments[len(l.segments)-1].indexPath); err != nil {
						return err
					}
					l.segments = l.segments[:len(l.segments)-1]
				}
				if err := os.Rename(filePath, filepath.Join(l.path, name[:n+len(l.logSuffix)])); err != nil {
					return err
				}
			}
			name = name[:n+len(l.logSuffix)]
		}
		l.segments = append(l.segments, &segment{
			offset:      offset,
			logPath:     filepath.Join(l.path, name),
			indexPath:   filepath.Join(l.path, name[:n]+l.indexSuffix),
			indexBuffer: make([]byte, 8),
			indexSpace:  l.indexSpace,
		})
		return nil
	})
	if err != nil {
		return err
	}
	if len(l.segments) > 0 {
		l.firstIndex = l.segments[0].offset + 1
		return l.resetLastSegment()
	}
	return nil
}

func (l *Log) appendSegment() (err error) {
	if err = l.closeLastSegment(); err != nil {
		return err
	}
	s := &segment{
		offset:      l.lastIndex,
		logPath:     filepath.Join(l.path, l.logName(l.lastIndex)),
		indexPath:   filepath.Join(l.path, l.indexName(l.lastIndex)),
		indexBuffer: make([]byte, 8),
		indexSpace:  l.indexSpace,
	}
	l.segments = append(l.segments, s)
	l.lastSegment = s
	if s.logFile, err = os.Create(s.logPath); err != nil {
		return err
	}
	if s.indexFile, err = os.Create(s.indexPath); err != nil {
		return err
	}
	if err = s.indexFile.Truncate(int64(l.indexSpace)); err != nil {
		return err
	}
	if err = s.indexFile.Sync(); err != nil {
		return err
	}
	if s.indexMmap, err = mmap.Open(mmap.Fd(s.indexFile), mmap.Fsize(s.indexFile), mmap.READ|mmap.WRITE); err != nil {
		return err
	}
	return
}

func (l *Log) resetLastSegment() (err error) {
	if err = l.closeLastSegment(); err != nil {
		return err
	}
	lastSegment := l.segments[len(l.segments)-1]
	l.lastSegment = lastSegment
	if lastSegment.logFile, err = os.OpenFile(lastSegment.logPath, os.O_RDWR, 0666); err != nil {
		return err
	}
	if n, err := lastSegment.logFile.Seek(0, os.SEEK_END); err != nil {
		return err
	} else if n <= 0 {
		l.lastIndex = lastSegment.offset
		return nil
	}
	if err := lastSegment.load(); err != nil {
		return err
	}
	l.lastIndex = lastSegment.offset + uint64(lastSegment.len)
	return nil
}

func (l *Log) closeLastSegment() (err error) {
	if l.lastSegment != nil {
		err = l.lastSegment.close()
	}
	return nil
}

func (l *Log) loadSegment(s *segment) (err error) {
	if s.len == 0 {
		if err := s.load(); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) Reset() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.reset()
}

func (l *Log) reset() (err error) {
	if err = l.empty(); err != nil {
		return err
	}
	l.initFirstIndex(1)
	return nil
}

func (l *Log) empty() (err error) {
	if err = l.close(); err != nil {
		return err
	}
	err = filepath.Walk(l.path, func(filePath string, info os.FileInfo, err error) error {
		name, n := info.Name(), l.nameLength
		if len(name) < n || info.IsDir() {
			return nil
		}
		_, err = l.parseSegmentName(name[:n])
		if err != nil {
			return nil
		}
		if name[n:n+len(l.logSuffix)] != l.logSuffix && name[n:n+len(l.indexSuffix)] != l.indexSuffix {
			return nil
		}
		if err := os.Remove(filePath); err != nil {
			return err
		}
		return nil
	})
	return err
}

func (l *Log) InitFirstIndex(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if index == 0 {
		return ErrZeroIndex
	}
	l.initFirstIndex(index)
	return nil
}

func (l *Log) initFirstIndex(index uint64) {
	l.firstIndex = index
	l.lastIndex = index - 1
	l.lastSegment = nil
	l.segments = l.segments[:0]
}

func (l *Log) Write(index uint64, data []byte) (err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if index == 0 {
		return ErrZeroIndex
	}
	if l.lastIndex > 0 && index != l.lastIndex+1 {
		return ErrOutOfOrder
	} else if l.lastIndex == 0 {
		l.firstIndex = index
		l.lastIndex = index - 1
	}
	if len(l.segments) == 0 {
		if err = l.appendSegment(); err != nil {
			return err
		}
	}
	end, err := l.lastSegment.logFile.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}
	offset := int(end)
	size := 10 + len(data)
	if cap(l.encodeBuffer) >= size {
		l.encodeBuffer = l.encodeBuffer[:size]
	} else {
		l.encodeBuffer = make([]byte, size)
	}
	n := code.EncodeVarint(l.encodeBuffer, uint64(len(data)))
	copy(l.encodeBuffer[n:], data)
	entryData := l.encodeBuffer[:int(n)+len(data)]
	if offset+len(l.writeBuffer)+len(entryData) > l.segmentSize || int(index-l.lastSegment.offset) > l.segmentEntries {
		if _, err := l.lastSegment.logFile.Write(l.writeBuffer); err != nil {
			return err
		}
		l.writeBuffer = l.writeBuffer[:0]
		if err := l.appendSegment(); err != nil {
			return err
		}
		l.lastSegment = l.segments[len(l.segments)-1]
		offset = 0
	}
	entries := index - l.lastSegment.offset
	code.EncodeUint64(l.lastSegment.indexBuffer, uint64(entries))
	copy(l.lastSegment.indexMmap, l.lastSegment.indexBuffer)
	code.EncodeUint64(l.lastSegment.indexBuffer, uint64(offset+len(l.writeBuffer)+len(entryData)))
	copy(l.lastSegment.indexMmap[entries*8:entries*8+8], l.lastSegment.indexBuffer)
	l.lastSegment.len = entries
	l.writeBuffer = append(l.writeBuffer, entryData...)
	l.lastIndex = index
	return nil
}

func (l *Log) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.flush()
}

func (l *Log) flush() error {
	if l.closed {
		return ErrClosed
	}
	if len(l.writeBuffer) > 0 {
		if _, err := l.lastSegment.logFile.Write(l.writeBuffer); err != nil {
			return err
		}
		l.writeBuffer = l.writeBuffer[:0]
		if err := l.lastSegment.logFile.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) Close() (err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	if err = l.flush(); err != nil {
		return err
	}
	return l.close()
}

func (l *Log) close() (err error) {
	for i := 0; i < len(l.segments); i++ {
		if err = l.segments[i].close(); err != nil {
			return err
		}
	}
	return
}

func (l *Log) FirstIndex() (index uint64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, ErrClosed
	}
	if l.lastIndex == l.firstIndex-1 {
		return l.lastIndex, nil
	}
	return l.firstIndex, nil
}

func (l *Log) LastIndex() (index uint64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, ErrClosed
	}
	return l.lastIndex, nil
}

func (l *Log) searchSegmentIndex(index uint64) int {
	low := 0
	high := len(l.segments) - 1
	for low <= high {
		mid := (low + high) / 2
		if index > l.segments[mid].offset {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return high
}

func (l *Log) IsExist(index uint64) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.checkIndex(index); err != nil {
		if err == ErrClosed {
			return false, err
		}
		return false, nil
	}
	return true, nil
}

func (l *Log) checkIndex(index uint64) error {
	if l.closed {
		return ErrClosed
	}
	if index == 0 || l.lastIndex == 0 || index < l.firstIndex || index > l.lastIndex {
		return ErrOutOfRange
	}
	return nil
}

func (l *Log) Read(index uint64) (data []byte, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.checkIndex(index); err != nil {
		return nil, err
	}
	segIndex := l.searchSegmentIndex(index)
	s := l.segments[segIndex]
	if err = l.loadSegment(s); err != nil {
		return nil, err
	}
	var start, end = s.readIndex(index)
	ret, _ := s.logFile.Seek(int64(start), os.SEEK_SET)
	entryData := make([]byte, end-start)
	n, err := s.logFile.ReadAt(entryData, ret)
	if err != nil {
		return nil, err
	}
	if len(entryData) == 0 {
		return nil, ErrShortBuffer
	}
	var size uint64
	n = int(code.DecodeVarint(entryData, &size))
	if uint64(len(entryData)-n) < size {
		return nil, ErrShortBuffer
	}
	return entryData[n:], nil
}

func (l *Log) Clean(index uint64) (err error) {
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
	for i := 0; i <= segIndex; i++ {
		l.segments[i].close()
		if err = os.Remove(l.segments[i].logPath); err != nil {
			return err
		}
		if err = os.Remove(l.segments[i].indexPath); err != nil {
			return err
		}
	}
	name := filepath.Join(l.path, l.logName(index-1))
	if err = os.Rename(cleanName, name); err != nil {
		return err
	}
	s.logPath = name
	s.offset = index - 1
	s.len = 0
	l.segments = l.segments[segIndex:]
	l.firstIndex = index
	if segIndex == len(l.segments)-1 {
		return l.resetLastSegment()
	}
	return nil
}

func (l *Log) Truncate(index uint64) (err error) {
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
	for i := segIndex; i < len(l.segments); i++ {
		l.segments[i].close()
		if err = os.Remove(l.segments[i].logPath); err != nil {
			return err
		}
		if err = os.Remove(l.segments[i].indexPath); err != nil {
			return err
		}
	}
	filePath := filepath.Join(l.path, l.logName(s.offset))
	if err = os.Rename(truncateName, filePath); err != nil {
		return err
	}
	s.logPath = filePath
	l.segments = l.segments[:segIndex+1]
	l.lastIndex = index
	return l.resetLastSegment()
}

func (l *Log) copy(srcName string, dstName string, offset, size int) (err error) {
	var srcFile, tmpFile *os.File
	if srcFile, err = os.Open(srcName); err != nil {
		return err
	}
	var m []byte
	if m, err = mmap.Open(mmap.Fd(srcFile), mmap.Fsize(srcFile), mmap.READ); err != nil {
		return err
	}
	tmpName := filepath.Join(l.path, "tmp")
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
	if tmpMmap, err = mmap.Open(mmap.Fd(tmpFile), mmap.Fsize(tmpFile), mmap.WRITE); err != nil {
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

func (l *Log) logName(offset uint64) string {
	return l.segmentName(offset) + l.logSuffix
}

func (l *Log) indexName(offset uint64) string {
	return l.segmentName(offset) + l.indexSuffix
}

func (l *Log) segmentName(offset uint64) string {
	return fmt.Sprintf("%0"+fmt.Sprintf("%d", l.nameLength)+"s", strconv.FormatUint(offset, l.base))
}

func (l *Log) parseSegmentName(segmentName string) (uint64, error) {
	offset, err := strconv.ParseUint(segmentName[:l.nameLength], l.base, 64)
	if err != nil {
		return 0, err
	}
	return offset, nil
}
