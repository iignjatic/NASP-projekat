package SSTable

import (
	"NASP-PROJEKAT/data"
)

// segment size - broj blokova
// block_size - broj zapisa u bloku

const BlockSize = 32 * 1024 //velicina bloka je 32 kilobajta

type DataSegment struct {
	Blocks      []*Block
	SegmentSize uint32 //koliko blokova sadrzi svaki segment
}

type Block struct {
	records   []*data.Record
	BlockSize uint32
	
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

	// for i := 0; i < int(len(records)); i++ {
	// 	if i%int(dataSegment.BlockSize) == 0 || i == 0 {
	// 		indexOfBlock += 1
	// 		dataSegment.Blocks[indexOfBlock].records = make([]*data.Record, dataSegment.BlockSize)
	// 		indexOfRecord = 0

	// 	}
	// 	dataSegment.Blocks[indexOfBlock].records[indexOfRecord] = records[i]
	// 	indexOfRecord += 1
	// }
	indexOfBlock = 0
	freeSpaceInBlock = BlockSize
	for i := 0; i < int(len(records)); i++ { //za svaki rekord provjeri moze li stati u blok
		dataSegment.Blocks[indexOfBlock].records = make([]*data.Record, arraySize/BlockSize) //blok sadrzi niz rekorda
		if recordSize(record[i] < )
		dataSegment.Blocks[indexOfBlock].records[indexOfRecord] = records[i]
	}
}
func getArraySize(records []*data.Record) uint32 { //vraca velicinu niza rekorda u bajtovima
	totalSize := 0
	recordSize := 0
	for i := 0; i < len(records); i++ {
		recordSize = 3*4 + records[i].KeySize + records[i].ValueSize + 1 + 2
		totalSize += recordSize
	}
	return uint32(totalSize)
}
func (sstable *SSTable) MakeSSTable(records []*data.Record) {
	arraySize := getArraySize(records)
	sstable.DataSegment.Blocks = make([]*Block,arraySize / BlockSize + 1) 
	for i := range sstable.DataSegment.Blocks {
		sstable.DataSegment.Blocks[i] = &Block{} // Inicijalizacija svakog bloka
	}

	sstable.DataSegment.makeSegment(records)

}
