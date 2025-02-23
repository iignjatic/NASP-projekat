package main

import (
	"NASP-PROJEKAT/data"
	"NASP-PROJEKAT/wal"
	"fmt"
)

func main() {
	//------------------------------------------------------CRC tests------------------------------------------------------
	fmt.Print("-----------------------------------------------------------------------------------------")
	fmt.Print("-----------------------------------------------------------------------------------------\n")
	record := data.NewRecord("key1", []byte("value1"))
	// Serializing the record
	data1, err := record.ToBytes()
	if err != nil {
		fmt.Printf("Serialization failed: %v\n", err)
		return
	}

	// Deserializing the record
	newRecord, err := data.FromBytes(data1)
	if err != nil {
		fmt.Printf("Deserialization failed: %v\n", err)
		return
	}

	// Output
	fmt.Printf("Original CRC: %d, Calculated CRC: %d\n", record.Crc, newRecord.Crc)
	fmt.Printf("Original Record: %+v\n", record)
	fmt.Printf("Deserialized Record: %+v\n", newRecord)


	//------------------------------------------------------WAL tests------------------------------------------------------
	fmt.Print("-----------------------------------------------------------------------------------------")
	fmt.Print("-----------------------------------------------------------------------------------------\n")
	fmt.Print("MESSAGES:\n")
	w := wal.NewWal()

	test := []*data.Record{
		data.NewRecord("key1", []byte("Ivana")),
		data.NewRecord("key2", []byte("Andjela")),
		data.NewRecord("key3", []byte("Elena")),
		data.NewRecord("key4", []byte("Aleksandar")),
		data.NewRecord("key5", []byte("-")),
		data.NewRecord("key5", []byte("1234567891123456789112345678911234567891123456789112345678911234567-00091123456789112345678911234567891123456789112345678911234567891123456789112345678911234567891123456781234567891123456789112345678911234567891123456789112345678911234567800091123456789112345678911211111111111111111111111111111111111111111111111111111111111111111111-0009112345678911234567891123456789112345678911234567891123456789112345678911234567891123456789112345678123456789112345678911234567891123456789112345678911234567891123456780009112345678911234567891121111111111111111111111111111111111111111111111111111111111111111111-456789112345678911234567891123456789112345678911234567891123456789112345678")),
		data.NewRecord("key1", []byte("Ivana")),
		data.NewRecord("key2", []byte("Andjela")),
		data.NewRecord("key3", []byte("Elena")),
		data.NewRecord("key4", []byte("Aleksandar")),
		data.NewRecord("key5", []byte("-")),
		data.NewRecord("key5", []byte("1234567891123456789112345678911234567891123456789112345678911234567-00091123456789112345678911234567891123456789112345678911234567891123456789112345678911234567891123456781234567891123456789112345678911234567891123456789112345678911234567800091123456789112345678911211111111111111111111111111111111111111111111111111111111111111111111-0009112345678911234567891123456789112345678911234567891123456789112345678911234567891123456789112345678123456789112345678911234567891123456789112345678911234567891123456780009112345678911234567891121111111111111111111111111111111111111111111111111111111111111111111-456789112345678911234567891123456789112345678911234567891123456789112345678")),
		data.NewRecord("key1", []byte("Ivana")),
		data.NewRecord("key2", []byte("Andjela")),
		data.NewRecord("key3", []byte("Elena")),
		data.NewRecord("key4", []byte("Aleksandar")),
		data.NewRecord("key5", []byte("-")),
		data.NewRecord("key3", []byte("Elena")),
		data.NewRecord("key4", []byte("Aleksandar")),
		data.NewRecord("key3", []byte("Elena")),
		data.NewRecord("key4", []byte("Aleksandar")),
		data.NewRecord("key3", []byte("Elena")),
		data.NewRecord("key4", []byte("Aleksandar")),
	}
	
	for i:=0; i<len(test); i++ {
		w.AddRecord(test[i])
	}

	for i:=0; i<len(w.Segments);i++ {
		fmt.Printf("\n----------------------Segment %d----------------------\n", w.Segments[i].ID)
		w.Segments[i].PrintBlocks()
	}
	fmt.Print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	fmt.Print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	defragmentedRecords, err := w.ReadAllSegments()
	if err != nil {
		fmt.Printf("Segment deserialization failed: %v\n", err)
		return
	}
	for i:=0; i<len(defragmentedRecords); i++ {
		fmt.Println(defragmentedRecords[i])
	}
}