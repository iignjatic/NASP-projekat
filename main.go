package main

import (
	"fmt"
	"nasp/wal"
)

func main() {
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
}
