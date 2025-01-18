package main

import (
	"fmt"
	"nasp/wal"
)

func main() {
	fmt.Print("\n------------------record.go tests------------------\n")
	record := wal.NewRecord("key1", []byte("value1"))
	// Serializing the record
	data, err := record.ToBytes()
	if err != nil {
		fmt.Printf("Serialization failed: %v\n", err)
		return
	}

	// Deserializing the record
	newRecord, err := wal.FromBytes(data)
	if err != nil {
		fmt.Printf("Deserialization failed: %v\n", err)
		return
	}

	// Output
	fmt.Printf("Original CRC: %d, Calculated CRC: %d\n", record.Crc, newRecord.Crc)
	fmt.Printf("Original Record: %+v\n", record)
	fmt.Printf("Deserialized Record: %+v", newRecord)


	fmt.Print("\n\n------------------block.go tests------------------\n")

	blockManager := wal.NewBlockManager()

	records := []*wal.Record{
		wal.NewRecord("key1", []byte("Ivana")),
		wal.NewRecord("key2", []byte("Andjela")),
		wal.NewRecord("key3", []byte("Elena")),
		wal.NewRecord("key4", []byte("Aleksandar")),
		wal.NewRecord("key5", []byte("Tijana")),
		wal.NewRecord("key5", []byte("Milan")),
	}

	for i:=0; i<len(records); i++ {
		blockManager.AddRecordToBlock(records[i])
	}

	blockManager.PrintBlocks()
}