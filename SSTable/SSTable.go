package SSTable

import (
	"NASP-PROJEKAT/data"
)

// segmentSize - broj blokova
// blockSize - broj zapisa u bloku

const BlockSize uint32 = 70 //velicina bloka je 32 kilobajta

type Block struct {
	records   []byte
	BlockSize uint32
}

type SSTable struct {
	DataSegment     *DataSegment
	Index           *Index
	Summary         *Summary
	BlockManager    *BlockManager
	DataFilePath    string
	IndexFilePath   string
	SummaryFilePath string
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

// vraca velicinu niza rekorda samarija  u bajtovima
func (sst *SSTable) getSummarySize(records []*data.Record) uint32 {
	var totalSize uint32 = 0
	var summaryCount = 0
	for i := 0; i < len(records); i++ {
		if summaryCount == int(sst.Summary.Sample) {
			recordSize := sst.Index.getRecordSize(records[i])
			totalSize += recordSize
			summaryCount = 0
		}
		summaryCount++
	}
	return uint32(totalSize)
}

func (sstable *SSTable) MakeSSTable(records []*data.Record) {
	//blokovi za data segment
	DataSize := sstable.getDataSize(records) //ukupna velicina data dijela
	sstable.DataSegment.Blocks = make([]*Block, DataSize/BlockSize*2)
	for i := range sstable.DataSegment.Blocks {
		sstable.DataSegment.Blocks[i] = &Block{} // Inicijalizacija svakog bloka
	}

	//blokovi za indeks
	IndexSize := sstable.getIndexSize(records) //ukupna velicina indeksa
	sstable.Index.Blocks = make([]*Block, IndexSize/BlockSize*2)
	for i := range sstable.Index.Blocks {
		sstable.Index.Blocks[i] = &Block{} // Inicijalizacija svakog bloka
	}

	//blokovi za summary
	sstable.Summary.Sample = 2 //OVDJE TREBA SAMPLE !!!!!!

	SummarySize := sstable.getSummarySize(records) //ukupna velicina samarija
	sstable.Summary.Blocks = make([]*Block, SummarySize/BlockSize*2)
	for i := range sstable.Summary.Blocks {
		sstable.Summary.Blocks[i] = &Block{} // Inicijalizacija svakog bloka
	}

	sstable.DataSegment.SegmentSize = uint32(len(sstable.DataSegment.Blocks))
	sstable.Index.SegmentSize = uint32(len(sstable.Index.Blocks))
	sstable.Summary.SegmentSize = uint32(len(sstable.Summary.Blocks))

	if len(records) > 0 {
		sstable.Summary.First = records[0].Key
		sstable.Summary.Last = records[len(records)-1].Key
	}

	sstable.MakeBlocks('d', records)
	sstable.MakeBlocks('i', records)
	sstable.MakeBlocks('s', records)

}

func (sst *SSTable) WriteSSTable() {
	var i uint32 = 0

	for i = 0; i < sst.DataSegment.SegmentSize; i++ {
		sst.BlockManager.writeBlock(sst.DataSegment.Blocks[i], sst.DataFilePath, i)
	}
	for i = 0; i < sst.Index.SegmentSize; i++ {
		sst.BlockManager.writeBlock(sst.Index.Blocks[i], sst.IndexFilePath, i)
	}
	for i = 0; i < sst.Summary.SegmentSize; i++ {
		sst.BlockManager.writeBlock(sst.Summary.Blocks[i], sst.SummaryFilePath, i)
	}

}

// t je tip blokova, za indeks, data ili summary
func (sst *SSTable) MakeBlocks(t byte, records []*data.Record) {
	i := 0 //rekord
	var pos uint32
	var indicator byte = 'a'
	var offsetIndex uint32 = 0
	var offsetSummary uint32 = 0
	var summaryCount int32 = -1
	tempBlockSize := BlockSize
	var recordBytes []byte
	var blocks []*Block

	if t == 'd' {
		blocks = sst.DataSegment.Blocks
	} else if t == 'i' {
		blocks = sst.Index.Blocks
	} else {
		blocks = sst.Summary.Blocks
	}

	for indexOfBlock := 0; indexOfBlock < len(blocks); indexOfBlock++ {
		if i >= len(records) { //upisali smo sve rekorde
			break
		}
		for tempBlockSize >= 0 && i < len(records) { //prolazak kroz jedan blok
			recordSize := sst.DataSegment.GetRecordSize(records[i])

			if t == 'd' { //d data segment
				recordBytes = sst.DataSegment.RecordToBytes(records[i], recordSize, indicator)
			} else if t == 'i' { //i indeks
				offsetIndex += recordSize                                                             //offset za velicinu cijelog rekorda
				recordSize = sst.Index.getRecordSize(records[i])                                      //azurira se na velicinu zapisa u indeksu
				recordBytes = sst.Index.recordToBytes(records[i], recordSize, indicator, offsetIndex) //zapis indeksa
			} else { //summary
				recordSize = sst.Index.getRecordSize(records[i])
				offsetSummary += recordSize
				summaryCount++
				if summaryCount == int32(sst.Summary.Sample) || summaryCount == 0 { //uzorak summaryja
					recordBytes = sst.Index.recordToBytes(records[i], recordSize, indicator, offsetSummary)
					summaryCount = 0
				} else {
					i += 1
					continue
				}
			}
			if recordSize < uint32(tempBlockSize) && indicator != 'm' && indicator != 'l' { //ako moze cijeli stati odmah
				indicator = 'a' //all kao citav rekord je stao
				blocks[indexOfBlock].records = append(blocks[indexOfBlock].records, recordBytes...)
				tempBlockSize -= uint32(recordSize) //smanjimo velicinu bloka za velicinu unijetog rekorda
				i += 1                              //prelazak na sledeci rekord

			} else if recordSize > uint32(tempBlockSize) && indicator != 'm' { //gigant je
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
					padding := make([]byte, tempBlockSize)
					blocks[indexOfBlock].records = append(blocks[indexOfBlock].records, padding...)
					indexOfBlock += 1
					tempBlockSize = BlockSize
					blocks[indexOfBlock].records = append(blocks[indexOfBlock].records, recordBytes...)
					tempBlockSize -= recordSize
					i += 1
				}

			} else if indicator == 'm' { //middle gigant
				recordSize = recordSize - BlockSize
				if recordSize < uint32(BlockSize) {
					indicator = 'l'
					//recordBytes := recordToBytes(records[i], recordSize, indicator)
					blocks[indexOfBlock].records = append(blocks[indexOfBlock].records, recordBytes[pos:]...)
					i += 1
					indicator = 'a'
					padding := make([]byte, BlockSize-recordSize)
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
				} else if t == 'i' {
					recordBytes = sst.Index.recordToBytes(records[i], recordSize, indicator, offsetIndex)
				} else {
					recordBytes = sst.Index.recordToBytes(records[i], recordSize, indicator, offsetSummary)
				}
				blocks[indexOfBlock].records = append(blocks[indexOfBlock].records, recordBytes...)
				tempBlockSize = BlockSize
				i += 1

			} else {
				break
			}
		}
	}
}
