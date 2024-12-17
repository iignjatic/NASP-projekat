package main

import (
	"NASP-PROJEKAT/SSTable"
	"NASP-PROJEKAT/data"
)

func main() {

	dataSeg := &SSTable.DataSegment{
		FilePath: "../SSTable/f.bin",
	}

	// Kreiranje SSTable-a
	sst := &SSTable.SSTable{
		DataSegment: dataSeg,
	}

	records := []data.Record{
		{Crc: 12345, KeySize: 4, ValueSize: 5, Key: "key1", Value: []byte("val1"), Tombstone: false, Timestamp: "2024-06-14"},
		{Crc: 67890, KeySize: 4, ValueSize: 5, Key: "key2", Value: []byte("val2"), Tombstone: true, Timestamp: "2024-06-15"},
		{Crc: 54321, KeySize: 4, ValueSize: 5, Key: "key3", Value: []byte("val3"), Tombstone: false, Timestamp: "2024-06-16"},
		{Crc: 12345, KeySize: 4, ValueSize: 5, Key: "key1", Value: []byte("val1"), Tombstone: false, Timestamp: "2024-06-14"},
		{Crc: 67890, KeySize: 4, ValueSize: 5, Key: "key2", Value: []byte("val2"), Tombstone: true, Timestamp: "2024-06-15"},
	}

	var recordPtrs []*data.Record
	for i := range records {
		recordPtrs = append(recordPtrs, &records[i])
	}
	sst.DataSegment.MakeSegment(recordPtrs)

	//table.MakeSSTable(recordPtrs)

	//table.Index.MakeIndex(table.DataSegment.Blocks, table.DataSegment.SegmentSize)
	//fmt.Println("SSTable sa segmentima: ", table.DataSegment.Blocks)

}
