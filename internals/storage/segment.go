// A Segment is the file on disk - one slice of the log

package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type Segment struct {
	BaseOffset uint64
	NextOffset uint64
	File       *os.File
	Path       string
	MaxBytes   int64
	Index      *Index
}

func NewSegment(dir string, baseOffset uint64, maxBytes int64) (*Segment, error) {
	path := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return nil, err
	}

	stat, _ := f.Stat()
	next := baseOffset
	if stat.Size() > 0 {
		next = baseOffset + uint64(countRecords(f))
	}

	idx, _ := NewIndex(dir, baseOffset)

	return &Segment{
		BaseOffset: baseOffset,
		NextOffset: next,
		File:       f,
		Path:       path,
		MaxBytes:   maxBytes,
		Index:      idx,
	}, nil

}

func (s *Segment) Append(rec *Record) (uint64, error) {
	rec.Offset = s.NextOffset
	data := rec.Serialize()

	lengthBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(lengthBuf, uint64(len(data)))

	pos, _ := s.File.Seek(0, io.SeekCurrent)

	if _, err := s.File.Write(lengthBuf); err != nil {
		return 0, err
	}

	if _, err := s.File.Write(data); err != nil {
		return 0, err
	}

	s.Index.Write(rec.Offset, pos)

	s.NextOffset++
	s.File.Sync()
	return rec.Offset, nil
}

func (s *Segment) ReadFrom(offset uint64, count int) ([]*Record, error) {
	_, err := s.File.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	var records []*Record
	var currentOffset uint64 = s.BaseOffset

	for {
		var lenBuf [8]byte
		_, err := s.File.Read(lenBuf[:])
		if err != nil {
			break
		}
		size := binary.BigEndian.Uint64(lenBuf[:])
		data := make([]byte, size)
		_, err = s.File.Read(data)
		if err != nil {
			break
		}
		if currentOffset >= offset {
			rec, _ := DeserializeRecord(currentOffset, data)
			records = append(records, rec)
			if count > 0 && len(records) >= count {
				break
			}
		}
		currentOffset++
	}

	return records, nil

}

func formatSegmentFilename(baseOffset uint64) string {
	return fmt.Sprintf("%020d.log", baseOffset)
}

func countRecords(f *os.File) int {
	var count int

	_, _ = f.Seek(0, 0)
	for {
		var lenBuf [8]byte
		_, err := f.Read(lenBuf[:])
		if err != nil {
			break
		}
		size := binary.BigEndian.Uint64(lenBuf[:])
		_, err = f.Seek(int64(size), 1)
		if err != nil {
			break
		}
		count++
	}

	return count

}
