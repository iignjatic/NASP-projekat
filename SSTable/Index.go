package SSTable

import (
	"NASP-PROJEKAT/data"
)

type Index struct {
	IndexTable map[string]uint32
}

func (index *Index) MakeIndex(records []*data.Record) {
	var offset uint32 = 0
	// pos = 0 //za citanje rekorda
	// var key string
	// var counter uint32 = 0      //masovni brojac
	// var blockCounter uint32 = 0 //brojac unutar bloka
	// for i := 0; i < len(blocks); i++ {
	// 	blockCounter = pos
	// 	records := blocks[i].records
	// 	tempBlockSize := blockSize

	// 	for j=0; j < blockSize; j ++{
	// 	for tempBlockSize >= 0 {
	// 		keySize := binary.LittleEndian.Uint32(records[pos+4 : pos+8])

	// 		valueSize := binary.LittleEndian.Uint32(records[pos+8 : pos+12])

	// 		key = string(records[pos+13 : pos+13+keySize])

	// 		recordSize := 4*3 + keySize + valueSize + 1 + 1 + 10
	// 		counter += recordSize      //prva tri polja rekorda
	// 		blockCounter += recordSize //prva tri polja rekorda
	// 		tempBlockSize -= recordSize
	// 		pos += blockCounter + 1

	// 	}
	// 	pos = -tempBlockSize
	// 	indexTable[key] = offset
	// 	offset = counter
	// }
	for i := 0; i < len(records); i++ {
		recordSize := getRecordSize(records[i])
		key := records[i].Key
		index.IndexTable[key] = offset
		offset += recordSize
	}
}
