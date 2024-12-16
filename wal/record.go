package wal

import (
	"encoding/binary"
	"unsafe"
)

type Record struct {
	Crc       uint32
	KeySize   uint32
	ValueSize uint32
	Key       string
	Value     []byte
	Tombstone bool
	Timestamp string
}

func (r *Record) ToBytes() ([]byte, error) {
	totalSize := int(unsafe.Sizeof(r.Crc)) + 
							 int(unsafe.Sizeof(r.KeySize)) + 
							 int(unsafe.Sizeof(r.ValueSize)) + 
							 len(r.Key) + 
							 len(r.Value) + 
							 1 +
							 len(r.Timestamp)

	bytesArray := make([]byte, totalSize)
	offset := 0

	binary.LittleEndian.PutUint32(bytesArray[offset:], r.ValueSize)
	offset += int(unsafe.Sizeof(r.ValueSize))

	copy(bytesArray[offset:], r.Key)
	offset += len(r.Key)

	if r.Tombstone {
		bytesArray[offset] = 1
	} else {
		bytesArray[offset] = 0
	}
	offset++

	copy(bytesArray[offset:], r.Timestamp)

	return bytesArray, nil
}

func FromBytes(data []byte) (*Record, error) {
	offset := 0

	crc := binary.LittleEndian.Uint32(data[offset:])
	offset += int(unsafe.Sizeof(crc))

	keySize := binary.LittleEndian.Uint32(data[offset:])
	offset += int(unsafe.Sizeof(keySize))

	valueSize := binary.LittleEndian.Uint32(data[offset:])
	offset += int(unsafe.Sizeof(valueSize))

	key := string(data[offset : offset + int(keySize)])
	offset += int(keySize)

	value := data[offset : offset + int(valueSize)]
	offset += int(valueSize)

	tombstone := data[offset] == 1
	offset++

	timestamp := string(data[offset] + 1)

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