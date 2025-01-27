package SSTable

import (
	"NASP-PROJEKAT/data"
	"encoding/binary"
)

type Index struct {
	Blocks        []*data.Block
	SegmentSize   uint64
	IndexFilePath string
	//	IndexTable    map[string]uint32
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

func (index *Index) recordToBytes(record *data.Record, size uint64, offset uint64) []byte {
	recordBytes := make([]byte, size)
	var keySize uint64 = record.KeySize
	var key string = record.Key

	binary.LittleEndian.PutUint64(recordBytes[0:], keySize)
	copy(recordBytes[data.KEY_SIZE:], []byte(key))
	binary.LittleEndian.PutUint64(recordBytes[data.KEY_SIZE+keySize:], offset)

	return recordBytes
}

func (index *Index) getRecordSize(record *data.Record) uint64 {
	return 8 + data.KEY_SIZE + record.KeySize //key, key size i offset
}
