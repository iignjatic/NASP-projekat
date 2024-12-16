package wal

import "fmt"

const BlockSize = 1024

type Block struct {
	ID              int
	Records         []byte
	FullCapacity    uint32
	CurrentCapacity uint32
}

func NewBlock(BlockID, FullCapacity int, records []*Record) Block {
	var allRecords []byte
	for _, record := range records {
		recordBytes, err := record.ToBytes()
		if err != nil {
			fmt.Printf("Error serializing record: %v\n", err)
			continue
		}
		allRecords = append(allRecords, recordBytes...)
	}

	return Block{
		ID:              BlockID,
		Records:         allRecords,
		FullCapacity:    uint32(FullCapacity),
		CurrentCapacity: uint32(len(records)),
	}
}