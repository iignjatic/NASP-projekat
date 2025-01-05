package SSTable

import (
	"NASP-PROJEKAT/data"
	"encoding/binary"
)

type Index struct {
	IndexTable    map[string]uint32
	Blocks        []*Block
	SegmentSize   uint32
	IndexFilePath string
}

func (index *Index) MakeIndex(records []*data.Record) {
	// var offset uint32 = 0

	// for i := 0; i < len(records); i++ {
	// 	recordSize := getRecordSize(records[i])
	// 	key := records[i].Key
	// 	index.IndexTable[key] = offset
	// 	offset += recordSize
	// }
}

func (index *Index) recordToBytes(record *data.Record, size uint32, indicator byte, offset uint32) []byte {
	recordBytes := make([]byte, size)
	var keySize uint32 = record.KeySize
	var key string = record.Key

	binary.LittleEndian.PutUint32(recordBytes[0:], keySize)
	copy(recordBytes[4:], []byte(key))
	binary.LittleEndian.PutUint32(recordBytes[4+keySize:], offset)

	return recordBytes
}

func (index *Index) getRecordSize(record *data.Record) uint32 {
	return 2*4 + record.KeySize
}
