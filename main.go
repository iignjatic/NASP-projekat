package main

import (
	"nasp/wal"
)

func main() {
	bufferPool := wal.NewBufferPool(3)
	records1 := []*wal.Record{
		{Crc: 1234, KeySize: 5, ValueSize: 8, Key: "key1", Value: []byte{1, 2, 3, 4, 5}, Tombstone: false, Timestamp: "2024-12-16T10:00:00"},
		{Crc: 5678, KeySize: 5, ValueSize: 7, Key: "key2", Value: []byte{6, 7, 8, 9, 10}, Tombstone: false, Timestamp: "2024-12-16T10:05:00"},
	}
	bufferPool.AddBlock(1, records1)

	records2 := []*wal.Record{
		{Crc: 91011, KeySize: 3, ValueSize: 5, Key: "key3", Value: []byte{11, 12, 13, 14, 15}, Tombstone: true, Timestamp: "2024-12-16T10:10:00"},
	}
	bufferPool.AddBlock(2, records2)

	bufferPool.FlushToDisk()	// simulation
}
