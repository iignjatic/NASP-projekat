package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

type Record struct {
	Crc       uint32
	KeySize   uint64
	ValueSize uint64
	Key       string
	Value     []byte
	Tombstone bool
	Timestamp string
}

func NewRecord(key string, value []byte, tombstone bool) *Record {
	timestamp := time.Now().UTC().Format(time.RFC3339)
	return &Record{
		Crc: 0, // computed during serialization
		KeySize: uint64(len(key)),
		ValueSize: uint64(len(value)),
		Key: key,
		Value: value,
		Tombstone: tombstone,
		Timestamp: timestamp,
	}
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func (r *Record) ToBytes() ([]byte, error) {
	keySize := len(r.Key)
	valueSize := len(r.Value)
	timestampSize := len(r.Timestamp)
	totalSize := 4 + 8 + 8 + keySize + valueSize + 1 + timestampSize

	bytesArray := make([]byte, totalSize)
	offset := 0
	
	// Placeholder for CRC
	offset += 4

	binary.LittleEndian.PutUint64(bytesArray[offset:], uint64(keySize))
	offset += 8
	binary.LittleEndian.PutUint64(bytesArray[offset:], uint64(valueSize))
	offset += 8

	// Write Key and Value
	copy(bytesArray[offset:], r.Key)
	offset += keySize
	copy(bytesArray[offset:], r.Value)
	offset += valueSize

	if r.Tombstone {
		bytesArray[offset] = 1
	} else {
		bytesArray[offset] = 0
	}
	offset++

	copy(bytesArray[offset:], []byte(r.Timestamp))

	crc := CRC32(bytesArray[4:])
	r.Crc = crc
	binary.LittleEndian.PutUint32(bytesArray[0:], crc)

	return bytesArray, nil
}

func FromBytes(data []byte) (*Record, error) {
	offset := 0

	crc := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	if offset+8 > len(data) {
		return nil, fmt.Errorf("insufficient data for keySize")
	}
	keySize := binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	if offset+8 > len(data) {
		return nil, fmt.Errorf("insufficient data for valueSize")
	}
	valueSize := binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	if offset+int(keySize) > len(data) {
		return nil, fmt.Errorf("insufficient data for key")
	}
	key := string(data[offset : offset + int(keySize)])
	offset += int(keySize)

	if offset+int(valueSize) > len(data) {
		return nil, fmt.Errorf("insufficient data for value")
	}
	value := data[offset : offset + int(valueSize)]
	offset += int(valueSize)

	if offset+1 > len(data) {
		return nil, fmt.Errorf("insufficient data for tombstone")
	}
	tombstone := data[offset] == 1
	offset++

	if offset > len(data) {
		return nil, fmt.Errorf("insufficient data for timestamp")
	}
	timestamp := string(data[offset:])
	
	calculatedCrc := CRC32(data[4:])
	if crc != calculatedCrc {
		return nil, fmt.Errorf("crc mismatch: expected %d, got %d", crc, calculatedCrc)
	}
	
	return &Record{
		Crc: crc,
		KeySize: keySize,
		ValueSize: valueSize,
		Key: key,
		Value: value,
		Tombstone: tombstone,
		Timestamp: timestamp,
	}, nil
}