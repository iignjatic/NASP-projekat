package SSTable

import (
	"NASP-PROJEKAT/data"
	"encoding/binary"
	"os"
)

type DataSegment struct {
	data     []byte
	FilePath string
}

func (dataSegment *DataSegment) MakeSegment(records []*data.Record) {
	var recordSize uint32 = 0
	for i := 0; i < len(records); i++ {
		recordSize = getRecordSize(records[i])
		recordBytes := recordToBytes(records[i], recordSize)

		dataSegment.data = append(dataSegment.data, recordBytes...)
		dataSegment.WriteToFile(recordBytes)
	}

}
func (dataSegment *DataSegment) WriteToFile(data []byte) {
	file, err := os.OpenFile(dataSegment.FilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 077)

	if err != nil {
		panic(err)
	}
	_, err = file.Write(data)
	if err != nil {
		panic(err)
	}
	defer file.Close()
}

func recordToBytes(record *data.Record, size uint32) []byte {
	recordBytes := make([]byte, size)
	var crc uint32 = record.Crc
	var keySize uint32 = record.KeySize
	var valueSize uint32 = record.ValueSize
	var key string = record.Key
	var value []byte = record.Value
	var tombstone bool = record.Tombstone

	binary.LittleEndian.PutUint32(recordBytes[0:], crc)
	binary.LittleEndian.PutUint32(recordBytes[4:], keySize)
	binary.LittleEndian.PutUint32(recordBytes[8:], valueSize)
	copy(recordBytes[12:], []byte(key))
	copy(recordBytes[12+keySize:], value)
	if tombstone {
		recordBytes[12+keySize+valueSize] = 1
	} else {
		recordBytes[12+keySize+valueSize] = 0
	}
	return recordBytes
}

func getRecordSize(record *data.Record) uint32 {
	return 3*4 + record.KeySize + record.ValueSize + 1 + 1 + 10
}
