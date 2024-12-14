package SSTable

import (
	"NASP-PROJEKAT/data"
)

// segment size - broj blokova
// block_size - broj zapisa u bloku

type DataSegment struct {
	Blocks      []*Block
	SegmentSize uint32
	BlockSize   uint32
}

type Block struct {
	records []*data.Record
}

type Index struct {
}

type Summary struct {
}

type SSTable struct {
	DataSegment *DataSegment
}

func (dataSegment *DataSegment) makeSegment(records []*data.Record) {
	indexOfBlock := -1

	indexOfRecord := 0

	for i := 0; i < int(len(records)); i++ {
		if i%int(dataSegment.BlockSize) == 0 || i == 0 {
			indexOfBlock += 1
			dataSegment.Blocks[indexOfBlock].records = make([]*data.Record, dataSegment.BlockSize)
			indexOfRecord = 0

		}
		dataSegment.Blocks[indexOfBlock].records[indexOfRecord] = records[i]
		indexOfRecord += 1
	}
}

func (sstable *SSTable) MakeSSTable(records []*data.Record) {
	sstable.DataSegment.Blocks = make([]*Block, len(records)/int(sstable.DataSegment.BlockSize)+1)

	for i := range sstable.DataSegment.Blocks {
		sstable.DataSegment.Blocks[i] = &Block{} // Inicijalizacija svakog bloka
	}

	sstable.DataSegment.makeSegment(records)

}
