package SSTable

import (
	"NASP-PROJEKAT/data"
)

// segment size - broj blokova
// block_size - broj zapisa u bloku

const BlockSize = 32 //velicina bloka je 32 kilobajta

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

	// for i := 0; i < int(len(records)); i++ {
	// 	if i%int(dataSegment.BlockSize) == 0 || i == 0 {
	// 		indexOfBlock += 1
	// 		dataSegment.Blocks[indexOfBlock].records = make([]*data.Record, dataSegment.BlockSize)
	// 		indexOfRecord = 0

	// 	}
	// 	dataSegment.Blocks[indexOfBlock].records[indexOfRecord] = records[i]
	// 	indexOfRecord += 1
	// }
	indexOfBlock := 0
	tempBlockSize := BlockSize
	for i := 0; i < int(len(records)); i++ { //za svaki rekord provjeri moze li stati u blok
		recordSize := getRecordSize(records[i])
		//dataSegment.Blocks[indexOfBlock].records = []*data.Record //blok sadrzi niz rekorda

		if recordSize < uint32(tempBlockSize) {
			dataSegment.Blocks[indexOfBlock].records = append(dataSegment.Blocks[indexOfBlock].records, records[i])
			tempBlockSize -= int(recordSize)
		}

	}
}
func getRecordSize(record *data.Record) uint32 {
	return 3*4 + record.KeySize + record.ValueSize + 1 + 2
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

	sstable.DataSegment.makeSegment(records)

}
