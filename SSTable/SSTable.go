package SSTable

import (
	"encoding/binary"
)

// segment size - broj blokova
// block_size - broj zapisa u bloku

const BlockSize uint32 = 70 //velicina bloka je 32 kilobajta

// type DataSegment struct {
// 	Blocks      []*Block
// 	SegmentSize uint32 //koliko blokova sadrzi svaki segment
// }

type Block struct {
	records   []byte
	BlockSize uint32
}

type Index struct {
	key    string
	offset uint32
}

func (index *Index) MakeIndex(blocks []*Block, blockSize uint32) {
	var indexTable = map[string]uint32{}
	var offset uint32 = 0 //pocetak prvog rekorda u bloku
	var pos uint32
	pos = 0 //za citanje rekorda
	var key string
	var counter uint32 = 0      //masovni brojac
	var blockCounter uint32 = 0 //brojac unutar bloka
	for i := 0; i < len(blocks); i++ {
		blockCounter = pos
		records := blocks[i].records
		tempBlockSize := blockSize

		//for j=0; j < blockSize; j ++{
		for tempBlockSize >= 0 {
			keySize := binary.LittleEndian.Uint32(records[pos+4 : pos+8])

			valueSize := binary.LittleEndian.Uint32(records[pos+8 : pos+12])

			key = string(records[pos+12 : pos+12+keySize])

			counter += 4*3 + keySize + valueSize + 1 + 1 + 10      //prva tri polja rekorda
			blockCounter += 4*3 + keySize + valueSize + 1 + 1 + 10 //prva tri polja rekorda
			recordSize := 4*3 + keySize + valueSize + 1 + 1 + 10
			tempBlockSize -= recordSize
			pos += blockCounter

		}
		pos = -tempBlockSize
		indexTable[key] = offset
		offset = counter
	}
}

type Summary struct {
}

type SSTable struct {
	DataSegment *DataSegment

	Index *Index
}

// func (dataSegment *DataSegment) makeSegment(records []*data.Record) {

// 	// for i := 0; i < int(len(records)); i++ {
// 	// 	if i%int(dataSegment.BlockSize) == 0 || i == 0 {
// 	// 		indexOfBlock += 1
// 	// 		dataSegment.Blocks[indexOfBlock].records = make([]*data.Record, dataSegment.BlockSize)
// 	// 		indexOfRecord = 0

// 	// 	}
// 	// 	dataSegment.Blocks[indexOfBlock].records[indexOfRecord] = records[i]
// 	// 	indexOfRecord += 1
// 	// }
// 	i := 0 //rekord
// 	var pos uint32
// 	var indicator byte
// 	indicator = 'a'
// 	tempBlockSize := BlockSize
// 	dataSegment.SegmentSize = BlockSize

// 	for indexOfBlock := 0; indexOfBlock < len(dataSegment.Blocks); indexOfBlock++ {
// 		if i >= len(records) { //upisali smo sve rekorde
// 			break
// 		}
// 		for tempBlockSize >= 0 && i < len(records) { //prolazak kroz jedan blok

// 			//for i := j; i < int(len(records)); i++ { //za svaki rekord provjeri moze li stati u blok
// 			recordSize := getRecordSize(records[i])

// 			if recordSize < uint32(tempBlockSize) && indicator != 'm' && indicator != 'l' { //ako moze cijeli stati odmah
// 				indicator = 'a' //all kao citav rekord je stao
// 				recordBytes := recordToBytes(records[i], recordSize, indicator)
// 				dataSegment.Blocks[indexOfBlock].records = append(dataSegment.Blocks[indexOfBlock].records, recordBytes...)
// 				tempBlockSize -= uint32(recordSize) //smanjimo velicinu bloka za velicinu unijetog rekorda
// 				i += 1                              //prelazak na sledeci rekord

// 			} else if recordSize > uint32(tempBlockSize) && indicator != 'm' {
// 				indicator = 'f' //first
// 				recordBytes := recordToBytes(records[i], recordSize, indicator)
// 				dataSegment.Blocks[indexOfBlock].records = append(dataSegment.Blocks[indexOfBlock].records, recordBytes[0:tempBlockSize]...)
// 				recordSize -= uint32(tempBlockSize)
// 				pos = uint32(tempBlockSize)
// 				indicator = 'm'
// 				break

// 			} else if indicator == 'm' {
// 				if recordSize < uint32(BlockSize) {
// 					indicator = 'l'
// 					recordBytes := recordToBytes(records[i], recordSize, indicator)
// 					dataSegment.Blocks[indexOfBlock].records = append(dataSegment.Blocks[indexOfBlock].records, recordBytes[pos:]...)
// 					i += 1
// 					tempBlockSize = (uint32(BlockSize) - recordSize + pos)
// 					indicator = 'a'

// 				} else {
// 					recordBytes := recordToBytes(records[i], recordSize, indicator)
// 					dataSegment.Blocks[indexOfBlock].records = append(dataSegment.Blocks[indexOfBlock].records, recordBytes[pos:BlockSize]...)
// 					pos = pos + BlockSize
// 					tempBlockSize = BlockSize

// 				}

// 			} else if recordSize == BlockSize && indicator != 'm' {
// 				indicator = 'a' //all kao citav rekord je stao
// 				recordBytes := recordToBytes(records[i], recordSize, indicator)
// 				dataSegment.Blocks[indexOfBlock].records = append(dataSegment.Blocks[indexOfBlock].records, recordBytes...)
// 				tempBlockSize = BlockSize //smanjimo velicinu bloka za velicinu unijetog rekorda
// 				i += 1                    //prelazak na sledeci rekord

// 			} else {
// 				break
// 			}
// 		}

// 		//}
// 	}

// }

// func getArraySize(records []*data.Record) uint32 { //vraca velicinu niza rekorda u bajtovima
// 	var totalSize uint32 = 0
// 	for i := 0; i < len(records); i++ {
// 		recordSize := getRecordSize(records[i])
// 		totalSize += recordSize
// 	}
// 	return uint32(totalSize)
// }

// func (sstable *SSTable) MakeSSTable(records []*data.Record) {
// 	arraySize := getArraySize(records)
// 	sstable.DataSegment.Blocks = make([]*Block, arraySize/BlockSize+1)
// 	for i := range sstable.DataSegment.Blocks {
// 		sstable.DataSegment.Blocks[i] = &Block{} // Inicijalizacija svakog bloka
// 	}

// 	sstable.DataSegment.makeSegment(records)

// }
