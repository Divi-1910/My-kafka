package storage

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
)

type Index struct {
	File  *os.File
	Path  string
	Mu    sync.Mutex
	Table map[uint64]int64
}

func NewIndex(dir string, baseOffset uint64) (*Index, error) {
	path := filepath.Join(dir, formatIndexFilename(baseOffset))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	idx := &Index{
		File:  f,
		Path:  path,
		Table: make(map[uint64]int64),
	}

	idx.load()
	return idx, nil
}

func (idx *Index) load() {
	idx.Mu.Lock()
	defer idx.Mu.Unlock()
	stat, _ := idx.File.Stat()
	if stat.Size() == 0 {
		return
	}

	var offset uint64
	var position int64

	idx.File.Seek(0, 0)

	for {
		if err := binary.Read(idx.File, binary.BigEndian, &offset); err != nil {
			break
		}
		if err := binary.Read(idx.File, binary.BigEndian, &position); err != nil {
			break
		}
		idx.Table[offset] = position
	}
}

func (idx *Index) Write(offset uint64, pos int64) error {
	idx.Mu.Lock()
	defer idx.Mu.Unlock()

	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], offset)
	binary.BigEndian.PutUint64(buf[8:16], uint64(pos))

	if _, err := idx.File.Write(buf); err != nil {
		return err
	}

	idx.Table[offset] = pos
	return idx.File.Sync()

}

func (idx *Index) Lookup(offset uint64) (int64, bool) {
	idx.Mu.Lock()
	defer idx.Mu.Unlock()

	pos, ok := idx.Table[offset]
	return pos, ok
}

func (idx *Index) Close() error {
	return idx.File.Close()
}

func formatIndexFilename(baseOffset uint64) string {
	return filepath.Base(formatSegmentFilename(baseOffset))[:20] + ".index"
}
