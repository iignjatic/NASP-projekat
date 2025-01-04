package SSTable

import (
	"NASP-PROJEKAT/data"
)

// segment size - broj blokova
// block_size - broj zapisa u bloku

const BlockSize uint32 = 70 //velicina bloka je 32 kilobajta

type Block struct {
	records   []byte
	BlockSize uint32
}

type SSTable struct {
	DataSegment *DataSegment

	Index   *Index
	Summary *Summary
}

func getArraySize(records []*data.Record) uint32 { //vraca velicinu niza rekorda u bajtovima
	var totalSize uint32 = 0
	for i := 0; i < len(records); i++ {
		recordSize := getRecordSize(records[i])
		totalSize += recordSize
	}
	return uint32(totalSize)
}

func (sstable *SSTable) MakeSSTable(records []*data.Record) {
	arraySize := getArraySize(records)
	sstable.DataSegment.Blocks = make([]*Block, arraySize/BlockSize+1)
	for i := range sstable.DataSegment.Blocks {
		sstable.DataSegment.Blocks[i] = &Block{} // Inicijalizacija svakog bloka
	}

	sstable.DataSegment.MakeSegment(records)

}
