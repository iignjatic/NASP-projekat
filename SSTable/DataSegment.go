package SSTable

import (
	"NASP-PROJEKAT/data"
	"encoding/binary"
)

type DataSegment struct {
	Blocks       []*data.Block
	SegmentSize  uint64
	DataFilePath string
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

//OVO JE VALJALO

// func (dataSegment *DataSegment) MakeSegment(records []*data.Record) {
// 	i := 0 //rekord
// 	var pos uint32
// 	var indicator byte
// 	indicator = 'a'
// 	tempBlockSize := BlockSize
// 	var recordBytes []byte

// 	for indexOfBlock := 0; indexOfBlock < len(dataSegment.Blocks); indexOfBlock++ {

// 		if i >= len(records) { //upisali smo sve rekorde
// 			break
// 		}
// 		for tempBlockSize >= 0 && i < len(records) { //prolazak kroz jedan blok

// 			//for i := j; i < int(len(records)); i++ { //za svaki rekord provjeri moze li stati u blok
// 			recordSize := getRecordSize(records[i])
// 			recordBytes = recordToBytes(records[i], recordSize, indicator)

// 			if recordSize < uint32(tempBlockSize) && indicator != 'm' && indicator != 'l' { //ako moze cijeli stati odmah
// 				indicator = 'a' //all kao citav rekord je stao
// 				dataSegment.Blocks[indexOfBlock].records = append(dataSegment.Blocks[indexOfBlock].records, recordBytes...)
// 				tempBlockSize -= uint32(recordSize) //smanjimo velicinu bloka za velicinu unijetog rekorda
// 				i += 1                              //prelazak na sledeci rekord

// 			} else if recordSize > uint32(tempBlockSize) && indicator != 'm' {
// 				//gigant je
// 				if recordSize > BlockSize {
// 					indexOfBlock += 1
// 					tempBlockSize = BlockSize
// 					indicator = 'f' //first
// 					recordBytes := recordToBytes(records[i], recordSize, indicator)
// 					dataSegment.Blocks[indexOfBlock].records = append(dataSegment.Blocks[indexOfBlock].records, recordBytes[0:tempBlockSize]...)
// 					recordSize -= uint32(tempBlockSize)
// 					pos = uint32(tempBlockSize)
// 					indicator = 'm'
// 					break
// 				} else {
// 					recordSize := getRecordSize(records[i])
// 					recordBytes = recordToBytes(records[i], recordSize, indicator)
// 					//peding
// 					padding := make([]byte, tempBlockSize-1)
// 					dataSegment.Blocks[indexOfBlock].records = append(dataSegment.Blocks[indexOfBlock].records, padding...)
// 					indexOfBlock += 1
// 					tempBlockSize = BlockSize
// 					dataSegment.Blocks[indexOfBlock].records = append(dataSegment.Blocks[indexOfBlock].records, recordBytes...)
// 					tempBlockSize -= recordSize
// 					i += 1
// 				}

// 			} else if indicator == 'm' {
// 				//middle gigant
// 				recordSize = recordSize - BlockSize
// 				if recordSize < uint32(BlockSize) {
// 					indicator = 'l'
// 					//recordBytes := recordToBytes(records[i], recordSize, indicator)
// 					dataSegment.Blocks[indexOfBlock].records = append(dataSegment.Blocks[indexOfBlock].records, recordBytes[pos:]...)
// 					i += 1

// 					indicator = 'a'
// 					padding := make([]byte, BlockSize-recordSize-1)
// 					dataSegment.Blocks[indexOfBlock].records = append(dataSegment.Blocks[indexOfBlock].records, padding...)
// 					indexOfBlock += 1
// 					tempBlockSize = BlockSize

// 				} else {
// 					//recordBytes := recordToBytes(records[i], recordSize, indicator)
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

// 	}

// }

func (dataSegment *DataSegment) RecordToBytes(record *data.Record, size uint64, indicator byte) []byte {
	recordBytes := make([]byte, size)
	var crc uint32 = record.Crc
	var keySize uint64 = record.KeySize
	var valueSize uint64
	var value []byte
	if record.Tombstone == true {
		valueSize = 0
		value = []byte{}
	} else {
		valueSize = record.ValueSize
		value = record.Value
	}
	var key string = record.Key
	var tombstone bool = record.Tombstone
	var timestamp = record.Timestamp

	binary.LittleEndian.PutUint32(recordBytes[0:], crc)
	binary.LittleEndian.PutUint64(recordBytes[data.CRC_SIZE:], keySize)
	binary.LittleEndian.PutUint64(recordBytes[data.CRC_SIZE+data.KEY_SIZE:], valueSize)
	//recordBytes = append(recordBytes, indicator)
	copy(recordBytes[data.KEY_SIZE+data.CRC_SIZE+data.VALUE_SIZE:], []byte(key))
	copy(recordBytes[data.KEY_SIZE+data.CRC_SIZE+data.VALUE_SIZE+keySize:], value)
	if tombstone {
		recordBytes[data.KEY_SIZE+data.CRC_SIZE+data.VALUE_SIZE+keySize+valueSize] = 1
	} else {
		recordBytes[data.KEY_SIZE+data.CRC_SIZE+data.VALUE_SIZE+keySize+valueSize] = 0
	}
	copy(recordBytes[data.KEY_SIZE+data.CRC_SIZE+data.VALUE_SIZE+keySize+valueSize+data.TOMBSTONE_SIZE:], []byte(timestamp))
	return recordBytes
}

func (dataSegment *DataSegment) GetRecordSize(record *data.Record) uint64 {
	return data.CRC_SIZE + data.KEY_SIZE + data.VALUE_SIZE + record.KeySize + record.ValueSize + data.TIMESTAMP_SIZE + data.TOMBSTONE_SIZE
}
