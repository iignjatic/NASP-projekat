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
	n_test := []*wal.Record{
		wal.NewRecord("key1", []byte("U sumama, drvece skripi pod tezinom snijega, a vjetar nosi miris borovine. Ptice pjevaju ujutro, dok se rosa sunca presijava na travi. Rijeke tiho teku, prelazeci kroz kamencice, a divlje zivotinje polako preplivaju prirodne staze. Planine uzdizu se u daljini.......")),
		wal.NewRecord("key1", []byte("Ivana")),
	}

	// p_test := []*wal.Record{
	// 	wal.NewRecord("key1", []byte("Ivana")),
	// 	wal.NewRecord("key2", []byte("Andjela")),
	// 	wal.NewRecord("key3", []byte("Elena")),
	// 	wal.NewRecord("key4", []byte("Aleksandar")),
	// 	wal.NewRecord("key4", []byte("Milan")),
	// }

	// b_test := []*wal.Record{
	// 	wal.NewRecord("key1", []byte("Ivana")),
	// 	wal.NewRecord("key2", []byte("Andjela")),
	// 	wal.NewRecord("key3", []byte("Elena")),
	// 	wal.NewRecord("key4", []byte("Aleksandar")),
	// 	wal.NewRecord("key5", []byte("Tijana")),
	// 	wal.NewRecord("key5", []byte("Sunce zalazi iza planinskog vrha.")),
	// 	wal.NewRecord("key5", []byte("Gledao sam Hunger Games dok sam ovo radio. Zanimljivost je 70 od 100")),
	// 	wal.NewRecord("key5", []byte("Milan")),
	// }

	for i:=0; i<len(n_test); i++ {
		blockManager.AddRecordToBlock(n_test[i])
	}

	blockManager.PrintBlocks()
}