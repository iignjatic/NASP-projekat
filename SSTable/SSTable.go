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
	DataSegment   *DataSegment
	Index         *Index
	Summary       *Summary
	BlockManager  *BlockManager
	DataFilePath  string
	IndexFilePath string
}

// vraca velicinu niza rekorda data segmenta u bajtovima
func (sst *SSTable) getDataSize(records []*data.Record) uint32 {
	var totalSize uint32 = 0
	for i := 0; i < len(records); i++ {
		recordSize := sst.DataSegment.GetRecordSize(records[i])
		totalSize += recordSize
	}
	return uint32(totalSize)
}

// vraca velicinu niza rekorda indeksa  u bajtovima
func (sst *SSTable) getIndexSize(records []*data.Record) uint32 {
	var totalSize uint32 = 0
	for i := 0; i < len(records); i++ {
		recordSize := sst.Index.getRecordSize(records[i])
		totalSize += recordSize
	}
	return uint32(totalSize)
}

func (sstable *SSTable) MakeSSTable(records []*data.Record) {
	//blokovi za data segment
	DataSize := sstable.getDataSize(records) //ukupna velicina data dijela
	sstable.DataSegment.Blocks = make([]*Block, DataSize/BlockSize+1)
	for i := range sstable.DataSegment.Blocks {
		sstable.DataSegment.Blocks[i] = &Block{} // Inicijalizacija svakog bloka
	}

	//blokovi za indeks
	IndexSize := sstable.getIndexSize(records) //ukupna velicina indeksa
	sstable.Index.Blocks = make([]*Block, IndexSize/BlockSize+1)
	for i := range sstable.Index.Blocks {
		sstable.Index.Blocks[i] = &Block{} // Inicijalizacija svakog bloka
	}

	sstable.DataSegment.SegmentSize = uint32(len(sstable.DataSegment.Blocks))
	sstable.Index.SegmentSize = uint32(len(sstable.Index.Blocks))

	sstable.MakeBlocks('d', records)
	sstable.MakeBlocks('i', records)

}

func (sst *SSTable) WriteSSTable() {
	var i uint32 = 0

	for i = 0; i < sst.DataSegment.SegmentSize; i++ {
		sst.BlockManager.writeBlock(sst.DataSegment.Blocks[i], sst.DataFilePath, i)
	}
	for i = 0; i < sst.Index.SegmentSize; i++ {
		sst.BlockManager.writeBlock(sst.Index.Blocks[i], sst.IndexFilePath, i)
	}

}

func (sst *SSTable) MakeBlocks(t byte, records []*data.Record) {
	i := 0 //rekord
	var pos uint32
	var indicator byte = 'a'
	var offset uint32 = 0
	tempBlockSize := BlockSize
	var recordBytes []byte
	var blocks []*Block

	//index ili data fajl
	if t == 'd' {
		blocks = sst.DataSegment.Blocks
	} else {
		blocks = sst.Index.Blocks
	}

	for indexOfBlock := 0; indexOfBlock < len(blocks); indexOfBlock++ {
		if i >= len(records) { //upisali smo sve rekorde
			break
		}
		for tempBlockSize >= 0 && i < len(records) { //prolazak kroz jedan blok
			recordSize := sst.DataSegment.GetRecordSize(records[i])

			if t == 'd' {
				recordBytes = sst.DataSegment.RecordToBytes(records[i], recordSize, indicator)
			} else {
				key := records[i].Key
				sst.Index.IndexTable[key] = offset
				offset += recordSize                                                             //offset za velicinu cijelog rekorda
				recordSize = sst.Index.getRecordSize(records[i])                                 //azurira se na velicinu zapisa u indeksu
				recordBytes = sst.Index.recordToBytes(records[i], recordSize, indicator, offset) //zapis indeksa
			}

			if recordSize < uint32(tempBlockSize) && indicator != 'm' && indicator != 'l' { //ako moze cijeli stati odmah
				indicator = 'a' //all kao citav rekord je stao
				blocks[indexOfBlock].records = append(blocks[indexOfBlock].records, recordBytes...)
				tempBlockSize -= uint32(recordSize) //smanjimo velicinu bloka za velicinu unijetog rekorda
				i += 1                              //prelazak na sledeci rekord

			} else if recordSize > uint32(tempBlockSize) && indicator != 'm' {
				//gigant je
				if recordSize > BlockSize {
					indexOfBlock += 1
					tempBlockSize = BlockSize
					indicator = 'f' //first
					//recordBytes := recordToBytes(records[i], recordSize, indicator)
					blocks[indexOfBlock].records = append(blocks[indexOfBlock].records, recordBytes[0:tempBlockSize]...)
					recordSize -= uint32(tempBlockSize)
					pos = uint32(tempBlockSize)
					indicator = 'm'
					break
				} else {
					if t == 'd' {
						recordSize = sst.DataSegment.GetRecordSize(records[i])
					} else {
						recordSize = sst.Index.getRecordSize(records[i])
					}
					//recordBytes = recordToBytes(records[i], recordSize, indicator)
					//peding
					padding := make([]byte, tempBlockSize-1)
					blocks[indexOfBlock].records = append(blocks[indexOfBlock].records, padding...)
					indexOfBlock += 1
					tempBlockSize = BlockSize
					blocks[indexOfBlock].records = append(blocks[indexOfBlock].records, recordBytes...)
					tempBlockSize -= recordSize
					i += 1
				}

			} else if indicator == 'm' {
				//middle gigant
				recordSize = recordSize - BlockSize
				if recordSize < uint32(BlockSize) {
					indicator = 'l'
					//recordBytes := recordToBytes(records[i], recordSize, indicator)
					blocks[indexOfBlock].records = append(blocks[indexOfBlock].records, recordBytes[pos:]...)
					i += 1

					indicator = 'a'
					padding := make([]byte, BlockSize-recordSize-1)
					blocks[indexOfBlock].records = append(blocks[indexOfBlock].records, padding...)
					indexOfBlock += 1
					tempBlockSize = BlockSize

				} else {
					//recordBytes := recordToBytes(records[i], recordSize, indicator)
					blocks[indexOfBlock].records = append(blocks[indexOfBlock].records, recordBytes[pos:BlockSize]...)
					pos = pos + BlockSize
					tempBlockSize = BlockSize

				}

			} else if recordSize == BlockSize && indicator != 'm' {
				indicator = 'a'
				if t == 'd' {
					recordBytes = sst.DataSegment.RecordToBytes(records[i], recordSize, indicator)
				} else {
					recordBytes = sst.Index.recordToBytes(records[i], recordSize, indicator, offset)
				}
				blocks[indexOfBlock].records = append(blocks[indexOfBlock].records, recordBytes...)
				tempBlockSize = BlockSize //smanjimo velicinu bloka za velicinu unijetog rekorda
				i += 1

			} else {
				break
			}
		}

	}

}
