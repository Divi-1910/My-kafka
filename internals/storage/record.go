// A record encodes key - value - timestamp as bytes

package storage

import (
	"bytes"
	"encoding/binary"
	"time"
)

type Record struct {
	Offset    uint64
	Timestamp int64
	Key       []byte
	Value     []byte
}

// convert record to raw bytes
func (rec *Record) Serialize() []byte {
	buf := new(bytes.Buffer)

	// Write length-prefixed fields: [keyLen][key][valueLen][value][timestamp]

	binary.Write(buf, binary.BigEndian, uint32(len(rec.Key)))
	buf.Write(rec.Key)

	binary.Write(buf, binary.BigEndian, uint32(len(rec.Value)))
	buf.Write(rec.Value)

	binary.Write(buf, binary.BigEndian, uint64(rec.Timestamp))
	return buf.Bytes()

}

func DeserializeRecord(offset uint64, data []byte) (*Record, error) {
	reader := bytes.NewReader(data)
	rec := &Record{
		Offset: offset,
	}

	var keyLen, valueLen uint32
	binary.Read(reader, binary.BigEndian, &keyLen)
	rec.Key = make([]byte, keyLen)
	reader.Read(rec.Key)

	binary.Read(reader, binary.BigEndian, &valueLen)
	rec.Value = make([]byte, valueLen)
	reader.Read(rec.Value)

	var timestamp uint64
	binary.Read(reader, binary.BigEndian, &timestamp)
	rec.Timestamp = int64(timestamp)

	return rec, nil
}

// to create a new record
func CreateNewRecord(key, value []byte) *Record {
	return &Record{
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UnixNano(),
	}
}
