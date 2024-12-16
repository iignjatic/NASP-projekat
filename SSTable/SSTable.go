package SSTable

import (
	"NASP-PROJEKAT/data"
	"encoding/binary"
)

// segment size - broj blokova
// block_size - broj zapisa u bloku

const BlockSize = 70 //velicina bloka je 32 kilobajta

type DataSegment struct {
	Blocks      []*Block
	SegmentSize uint32 //koliko blokova sadrzi svaki segment
}

type Block struct {
	records   []byte
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
	i := 0 //rekord
	var pos uint32
	var indicator byte
	indicator = 'a'
	tempBlockSize := BlockSize

	for indexOfBlock := 0; indexOfBlock < len(dataSegment.Blocks); indexOfBlock++ {
		if i >= len(records) { //upisali smo sve rekorde
			break
		}
		for tempBlockSize >= 0 && i < len(records) { //prolazak kroz jedan blok

			//for i := j; i < int(len(records)); i++ { //za svaki rekord provjeri moze li stati u blok
			recordSize := getRecordSize(records[i])

			if recordSize < uint32(tempBlockSize) && indicator != 'm' && indicator != 'l' { //ako moze cijeli stati odmah
				indicator = 'a' //all kao citav rekord je stao
				recordBytes := recordToBytes(records[i], recordSize, indicator)
				dataSegment.Blocks[indexOfBlock].records = append(dataSegment.Blocks[indexOfBlock].records, recordBytes...)
				tempBlockSize -= int(recordSize) //smanjimo velicinu bloka za velicinu unijetog rekorda
				i += 1                           //prelazak na sledeci rekord

			} else if recordSize > uint32(tempBlockSize) && indicator != 'm' {
				indicator = 'f' //first
				recordBytes := recordToBytes(records[i], recordSize, indicator)
				dataSegment.Blocks[indexOfBlock].records = append(dataSegment.Blocks[indexOfBlock].records, recordBytes[0:tempBlockSize]...)
				recordSize -= uint32(tempBlockSize)
				pos = uint32(tempBlockSize)
				indicator = 'm'
				break

			} else if indicator == 'm' {
				if recordSize < uint32(BlockSize) {
					indicator = 'l'
					recordBytes := recordToBytes(records[i], recordSize, indicator)
					dataSegment.Blocks[indexOfBlock].records = append(dataSegment.Blocks[indexOfBlock].records, recordBytes[pos:]...)
					i += 1
					tempBlockSize = int(uint32(BlockSize) - recordSize + pos)
					indicator = 'a'

				} else {
					recordBytes := recordToBytes(records[i], recordSize, indicator)
					dataSegment.Blocks[indexOfBlock].records = append(dataSegment.Blocks[indexOfBlock].records, recordBytes[pos:BlockSize]...)
					pos = pos + BlockSize
					tempBlockSize = BlockSize

				}

			} else if recordSize == BlockSize && indicator != 'm' {
				indicator = 'a' //all kao citav rekord je stao
				recordBytes := recordToBytes(records[i], recordSize, indicator)
				dataSegment.Blocks[indexOfBlock].records = append(dataSegment.Blocks[indexOfBlock].records, recordBytes...)
				tempBlockSize = BlockSize //smanjimo velicinu bloka za velicinu unijetog rekorda
				i += 1                    //prelazak na sledeci rekord

			} else {
				break
			}
		}

		//}
	}

}

func recordToBytes(record *data.Record, size uint32, ind byte) []byte {
	recordBytes := make([]byte, size)
	var crc uint32 = record.Crc
	var keySize uint32 = record.KeySize
	var valueSize uint32 = record.ValueSize
	var key string = record.Key
	var value []byte = record.Value
	var tombstone bool = record.Tombstone
	var indicator byte = ind

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
	recordBytes[13+keySize+valueSize] = indicator

	return recordBytes
}

func getRecordSize(record *data.Record) uint32 {
	return 3*4 + record.KeySize + record.ValueSize + 1 + 1 + 10
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
