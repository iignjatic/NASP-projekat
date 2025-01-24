package main

import (
	"NASP-PROJEKAT/SSTable"
	"NASP-PROJEKAT/data"
	"fmt"
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
		{Crc: 67890, KeySize: 4, ValueSize: 200, Key: "key2", Value: []byte("val2"), Tombstone: true, Timestamp: "2024-06-15"},
		{Crc: 54321, KeySize: 4, ValueSize: 140, Key: "key3", Value: []byte("val3"), Tombstone: false, Timestamp: "2024-06-16"},
		{Crc: 12345, KeySize: 4, ValueSize: 5, Key: "key4", Value: []byte("val4"), Tombstone: false, Timestamp: "2024-06-14"},
		{Crc: 67890, KeySize: 4, ValueSize: 20, Key: "key5", Value: []byte("val5"), Tombstone: true, Timestamp: "2024-06-15"},
		{Crc: 12345, KeySize: 4, ValueSize: 500, Key: "key6", Value: []byte("val6PERSA PERSIC"), Tombstone: false, Timestamp: "2024-06-14"},
	}

	var recordPtrs []*data.Record
	for i := range records {
		recordPtrs = append(recordPtrs, &records[i])
	}
	sst.MakeSSTable(recordPtrs)

	sst.Index = index
	sst.Summary = summary

	sst.WriteSSTable()

	//korisnik salje get zahtjev
	//prvo provjerimo da li se zapis nalazi u Memtable strukturi (ako je tu, vratimo odgovor)
	//nakon toga provjeravamo da li je se zapis nalazi u Cache strukturi (ako je tu, vratimo odgovor)
	//u glavnom main-u ce biti pozvana get metoda nad memtable, ako vrati false poziva se nad cache strukturom
	//ako vrati false poziva se nad sstable strukturom

	var key string = "key6"
	value := sst.Get(key)
	if value == nil {
		fmt.Println("Zapis nije pronadjen")
	} else {
		fmt.Println("Zapis je pronadjen : ", string(value))
	}

}
