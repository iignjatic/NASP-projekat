package main

import (
	"fmt"
	"nasp/wal"
)

func main() {
	record := wal.NewRecord("key1", []byte("value1"), false)
	// Serializing the record
	data, err := record.ToBytes()
	if err != nil {
		fmt.Printf("Serialization failed: %v\n", err)
		return
	}

	// Deserialize the record
	newRecord, err := wal.FromBytes(data)
	if err != nil {
		fmt.Printf("Deserialization failed: %v\n", err)
		return
	}

	// Output
	fmt.Printf("Original CRC: %d, Calculated CRC: %d\n", record.Crc, newRecord.Crc)
	fmt.Printf("Original Record: %+v\n", record)
	fmt.Printf("Deserialized Record: %+v", newRecord)

	// bufferPool := wal.NewBufferPool(3)
	// records1 := []*wal.Record{
	// 	{Crc: 1234, KeySize: 5, ValueSize: 8, Key: "key1", Value: []byte{1, 2, 3, 4, 5}, Tombstone: false, Timestamp: "2024-12-16T10:00:00"},
	// 	{Crc: 5678, KeySize: 5, ValueSize: 7, Key: "key2", Value: []byte{6, 7, 8, 9, 10}, Tombstone: false, Timestamp: "2024-12-16T10:05:00"},
	// }
	// bufferPool.AddBlock(1, records1)

	// records2 := []*wal.Record{
	// 	{Crc: 91011, KeySize: 3, ValueSize: 5, Key: "key3", Value: []byte{11, 12, 13, 14, 15}, Tombstone: true, Timestamp: "2024-12-16T10:10:00"},
	// }
	// bufferPool.AddBlock(2, records2)

	// bufferPool.FlushToDisk()	// simulation

	// // Creating the WAL with max 3 blocks by Segment
	// walDir := "./wal_logs"
	// walInstance := wal.NewWal(walDir, 3)

	// for i:=1; i<=10; i++ {
	// 	block := wal.NewBlock(i, wal.BlockSize, records1)
	// 	walInstance.AddBlock(block)
	// }

	// walInstance.FlushToDisk()
}
