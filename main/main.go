package main

import (
	"NASP-PROJEKAT/SSTable"
	"NASP-PROJEKAT/data"
	"fmt"
)

func main() {
	table := SSTable.SSTable{
		DataSegment: &SSTable.DataSegment{},
	}

	records := []data.Record{
		{Crc: 12345, KeySize: 3, ValueSize: 5, Key: "key1", Value: []byte("val1"), Tombstone: false, Timestamp: "2024-06-14"},
		{Crc: 67890, KeySize: 3, ValueSize: 5, Key: "key2", Value: []byte("val2"), Tombstone: true, Timestamp: "2024-06-15"},
		{Crc: 54321, KeySize: 3, ValueSize: 5, Key: "key3", Value: []byte("val3"), Tombstone: false, Timestamp: "2024-06-16"},
		{Crc: 12345, KeySize: 3, ValueSize: 5, Key: "key1", Value: []byte("val1"), Tombstone: false, Timestamp: "2024-06-14"},
		{Crc: 67890, KeySize: 3, ValueSize: 5, Key: "key2", Value: []byte("val2"), Tombstone: true, Timestamp: "2024-06-15"},
		{Crc: 54321, KeySize: 3, ValueSize: 5, Key: "key3", Value: []byte("val3"), Tombstone: false, Timestamp: "2024-06-16"},
		{Crc: 12345, KeySize: 3, ValueSize: 5, Key: "key1", Value: []byte("val1"), Tombstone: false, Timestamp: "2024-06-14"},
		{Crc: 67890, KeySize: 3, ValueSize: 5, Key: "key2", Value: []byte("val2"), Tombstone: true, Timestamp: "2024-06-15"},
		{Crc: 54321, KeySize: 3, ValueSize: 5, Key: "key3", Value: []byte("val3"), Tombstone: false, Timestamp: "2024-06-16"},
	}

	var recordPtrs []*data.Record
	for i := range records {
		recordPtrs = append(recordPtrs, &records[i])
	}
	table.MakeSSTable(recordPtrs)
	fmt.Println("SSTable sa segmentima: ", table.DataSegment.Blocks)

}
