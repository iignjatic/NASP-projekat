package SSTable

import (
	"NASP-PROJEKAT/data"
	"encoding/binary"
	"os"
)

type DataSegment struct {
	data        []byte
	Blocks      []*Block
	FilePath    string
	SegmentSize uint32
}

// func (dataSegment *DataSegment) MakeSegment(records []*data.Record) {
// 	var recordSize uint32 = 0
// 	for i := 0; i < len(records); i++ {
// 		recordSize = getRecordSize(records[i])
// 		recordBytes := recordToBytes(records[i], recordSize)

// 		dataSegment.data = append(dataSegment.data, recordBytes...)
// 		dataSegment.WriteToFile(recordBytes)
// 	}

// }

func (dataSegment *DataSegment) MakeSegment(records []*data.Record) {
	i := 0 //rekord
	var pos uint32
	var indicator byte
	indicator = 'a'
	tempBlockSize := BlockSize
	dataSegment.SegmentSize = BlockSize
	var recordBytes []byte

	for indexOfBlock := 0; indexOfBlock < len(dataSegment.Blocks); indexOfBlock++ {

		if i >= len(records) { //upisali smo sve rekorde
			break
		}
		for tempBlockSize >= 0 && i < len(records) { //prolazak kroz jedan blok

			//for i := j; i < int(len(records)); i++ { //za svaki rekord provjeri moze li stati u blok
			recordSize := getRecordSize(records[i])
			recordBytes = recordToBytes(records[i], recordSize, indicator)

			if recordSize < uint32(tempBlockSize) && indicator != 'm' && indicator != 'l' { //ako moze cijeli stati odmah
				indicator = 'a' //all kao citav rekord je stao
				dataSegment.Blocks[indexOfBlock].records = append(dataSegment.Blocks[indexOfBlock].records, recordBytes...)
				tempBlockSize -= uint32(recordSize) //smanjimo velicinu bloka za velicinu unijetog rekorda
				i += 1                              //prelazak na sledeci rekord

			} else if recordSize > uint32(tempBlockSize) && indicator != 'm' {
				//gigant je
				if recordSize > BlockSize {
					indexOfBlock += 1
					tempBlockSize = BlockSize
					indicator = 'f' //first
					recordBytes := recordToBytes(records[i], recordSize, indicator)
					dataSegment.Blocks[indexOfBlock].records = append(dataSegment.Blocks[indexOfBlock].records, recordBytes[0:tempBlockSize]...)
					recordSize -= uint32(tempBlockSize)
					pos = uint32(tempBlockSize)
					indicator = 'm'
					break
				} else {
					recordSize := getRecordSize(records[i])
					recordBytes = recordToBytes(records[i], recordSize, indicator)
					//peding
					padding := make([]byte, tempBlockSize-1)
					dataSegment.Blocks[indexOfBlock].records = append(dataSegment.Blocks[indexOfBlock].records, padding...)
					indexOfBlock += 1
					tempBlockSize = BlockSize
					dataSegment.Blocks[indexOfBlock].records = append(dataSegment.Blocks[indexOfBlock].records, recordBytes...)
					tempBlockSize -= recordSize
					i += 1
				}

			} else if indicator == 'm' {
				//middle gigant
				recordSize = recordSize - BlockSize
				if recordSize < uint32(BlockSize) {
					indicator = 'l'
					//recordBytes := recordToBytes(records[i], recordSize, indicator)
					dataSegment.Blocks[indexOfBlock].records = append(dataSegment.Blocks[indexOfBlock].records, recordBytes[pos:]...)
					i += 1

					indicator = 'a'
					padding := make([]byte, BlockSize-recordSize-1)
					dataSegment.Blocks[indexOfBlock].records = append(dataSegment.Blocks[indexOfBlock].records, padding...)
					indexOfBlock += 1
					tempBlockSize = BlockSize

				} else {
					//recordBytes := recordToBytes(records[i], recordSize, indicator)
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

func recordToBytes(record *data.Record, size uint32, indicator byte) []byte {
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
	recordBytes = append(recordBytes, indicator)
	copy(recordBytes[13:], []byte(key))
	copy(recordBytes[13+keySize:], value)
	if tombstone {
		recordBytes[13+keySize+valueSize] = 1
	} else {
		recordBytes[13+keySize+valueSize] = 0
	}
	return recordBytes
}

func getRecordSize(record *data.Record) uint32 {
	return 3*4 + record.KeySize + record.ValueSize + 1 + 1 + 10
}
