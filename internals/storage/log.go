// A collection of segments, append + read by offset.

// A log is a simple durable sequential and ordered file

package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type Log struct {
	Dir             string
	MaxSegmentBytes int64
	Segments        []*Segment
	Active          *Segment
	Mu              sync.Mutex
}

func NewLog(dir string, maxSegmentBytes int64) (*Log, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	log := &Log{
		Dir:             dir,
		MaxSegmentBytes: maxSegmentBytes,
	}

	if err := log.loadSegments(); err != nil {
		return nil, err
	}

	if len(log.Segments) == 0 {
		seg, err := NewSegment(dir, 0, maxSegmentBytes)
		if err != nil {
			return nil, err
		}
		log.Segments = append(log.Segments, seg)
		log.Active = seg
	} else {
		log.Active = log.Segments[len(log.Segments)-1]
	}

	return log, nil

}

func (l *Log) Append(key, value []byte) (uint64, error) {
	l.Mu.Lock()
	defer l.Mu.Unlock()

	rec := CreateNewRecord(key, value)
	offset, err := l.Active.Append(rec)
	if err != nil {
		return 0, err
	}

	stat, _ := l.Active.File.Stat()
	if stat.Size() >= l.MaxSegmentBytes {
		_ = l.rollSegment()
	}

	return offset, nil
}

func (l *Log) Read(offset uint64, count int) ([]*Record, error) {
	l.Mu.Lock()
	defer l.Mu.Unlock()

	var records []*Record

	for _, seg := range l.Segments {
		if offset < seg.NextOffset {
			recs, _ := seg.ReadFrom(offset, count-len(records))
			records = append(records, recs...)
			offset = seg.NextOffset
			if count > 0 && len(records) >= count {
				break
			}
		}
	}

	return records, nil
}

func (l *Log) rollSegment() error {
	base := l.Active.NextOffset
	newSeg, err := NewSegment(l.Dir, base, l.MaxSegmentBytes)
	if err != nil {
		return err
	}
	l.Active.File.Close()
	l.Segments = append(l.Segments, newSeg)
	l.Active = newSeg
	return nil
}

func (l *Log) loadSegments() error {
	files, err := filepath.Glob(filepath.Join(l.Dir, "*.log"))
	if err != nil {
		return err
	}

	sort.Strings(files)

	for _, file := range files {
		base := getBaseOffset(file)
		seg, err := NewSegment(l.Dir, base, l.MaxSegmentBytes)
		if err != nil {
			return err
		}
		l.Segments = append(l.Segments, seg)
	}

	return nil

}

func getBaseOffset(path string) uint64 {
	filename := filepath.Base(path)
	var base uint64
	fmt.Sscanf(filename, "%020d.log", &base)
	return base
}
