package main

import (
	"NASP-PROJEKAT/SSTable"
	"NASP-PROJEKAT/data"
)

func main() {

	dataSeg := &SSTable.DataSegment{}

	index := &SSTable.Index{}

	summary := &SSTable.Summary{}

	blockManager := &SSTable.BlockManager{}

	// Kreiranje SSTable-a
	sst := &SSTable.SSTable{
		DataSegment:     dataSeg,
		Index:           index,
		Summary:         summary,
		BlockManager:    blockManager,
		DataFilePath:    "../SSTable/files/data.bin",
		IndexFilePath:   "../SSTable/files/index.bin",
		SummaryFilePath: "../SSTable/files/summary.bin",
	}

	records := []data.Record{
		{Crc: 12345, KeySize: 4, ValueSize: 5, Key: "key1", Value: []byte("val1"), Tombstone: false, Timestamp: "2024-06-14"},
		{Crc: 67890, KeySize: 4, ValueSize: 5, Key: "key2", Value: []byte("val2"), Tombstone: true, Timestamp: "2024-06-15"},
		{Crc: 54321, KeySize: 30, ValueSize: 30, Key: "key3", Value: []byte("val3"), Tombstone: false, Timestamp: "2024-06-16"},
		{Crc: 12345, KeySize: 4, ValueSize: 5, Key: "key4", Value: []byte("val4"), Tombstone: false, Timestamp: "2024-06-14"},
		{Crc: 67890, KeySize: 4, ValueSize: 5, Key: "key5", Value: []byte("val5"), Tombstone: true, Timestamp: "2024-06-15"},
		{Crc: 12345, KeySize: 4, ValueSize: 5, Key: "key1", Value: []byte("val1"), Tombstone: false, Timestamp: "2024-06-14"},
		{Crc: 67890, KeySize: 4, ValueSize: 5, Key: "key2", Value: []byte("val2"), Tombstone: true, Timestamp: "2024-06-15"},
		{Crc: 54321, KeySize: 30, ValueSize: 30, Key: "key3", Value: []byte("val3"), Tombstone: false, Timestamp: "2024-06-16"},
		{Crc: 12345, KeySize: 4, ValueSize: 5, Key: "key4", Value: []byte("val4"), Tombstone: false, Timestamp: "2024-06-14"},
		{Crc: 67890, KeySize: 4, ValueSize: 5, Key: "key5", Value: []byte("val5"), Tombstone: true, Timestamp: "2024-06-15"},
	}

	var recordPtrs []*data.Record
	for i := range records {
		recordPtrs = append(recordPtrs, &records[i])
	}
	sst.MakeSSTable(recordPtrs)

	sst.Index = index
	sst.Summary = summary

	sst.WriteSSTable()

}
