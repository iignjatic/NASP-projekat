package wal

import "fmt"

const BLOCK_SIZE = 300

type Block struct {
	ID              int
	Records         []*Record
	FullCapacity    uint64
	CurrentCapacity uint64
}

type BlockManager struct {
	Blocks []*Block
}

func NewBlock(BlockID int) *Block {
	return &Block{
		ID:              BlockID,
		Records:         []*Record{},
		FullCapacity:    BLOCK_SIZE,
		CurrentCapacity: 0,
	}
}

func NewBlockManager() *BlockManager {
	firstBlock := NewBlock(0)
	return &BlockManager{
		Blocks: []*Block{firstBlock},
	}
}

// Before adding a new Record to the Block, it have to be decided which operation should work: PADDING - p or FRAGMENTATION - f, nothing mentioned - n which means record fits just fine
func ChosenOperation(currentBlock *Block, record *Record) byte {
	remainingBlockCapacity := currentBlock.FullCapacity - currentBlock.CurrentCapacity

	recordFullSize := CalculateRecordSize(record)
	recordValueSize := record.ValueSize
	remainingRecordSize := recordFullSize - int(recordValueSize)

	if remainingBlockCapacity == uint64(recordFullSize) {
		return 'n'
	} else if remainingBlockCapacity < uint64(recordFullSize) {
		// This means that CRC, Timestamp, Tombstone, Type, KeySize, ValueSize, Key and some part of the Value can fit in the Block
		if remainingBlockCapacity > uint64(remainingRecordSize) {
			return 'f'
		} else {
			return 'p'
		}
	// Record fits just fine
	} else {
		return 'p'
	}
}

func (bm *BlockManager) AddRecordToBlock(record *Record) {
	currentBlock := bm.Blocks[len(bm.Blocks)-1]
	operation := ChosenOperation(currentBlock, record)

	if operation == 'n' {
		// Racord fits just fine, save it to block
		currentBlock.Records = append(currentBlock.Records, record)
		currentBlock.CurrentCapacity += uint64(CalculateRecordSize(record))
	} else if operation == 'p' {
		// Clean all zeros from the value of the last added record and reduce the length of block
		if len(currentBlock.Records) > 0 {
			var zerosToDelete uint64
			lastAddedRecord := currentBlock.Records[len(currentBlock.Records) - 1]
			lastAddedRecord.Value, zerosToDelete = TrimZeros(lastAddedRecord.Value)
			lastAddedRecord.ValueSize = uint64(len(lastAddedRecord.Value))
			currentBlock.CurrentCapacity = currentBlock.CurrentCapacity - zerosToDelete
		}
		// Append zeros to the Value of the Record, and save that record to block
		remainingBlockCapacity := currentBlock.FullCapacity - currentBlock.CurrentCapacity
		numOfZeros := remainingBlockCapacity - uint64(CalculateRecordSize(record))
		padding := make([]byte, numOfZeros)
		record.Value = append(record.Value, padding...)
		record.ValueSize = uint64(len(record.Value))
		currentBlock.Records = append(currentBlock.Records, record)
		currentBlock.CurrentCapacity += uint64(CalculateRecordSize(record))
	}
}

func TrimZeros(data []byte) ([]byte, uint64) {
	var i uint64
	for len(data) > 0 && data[len(data)-1] == 0 {
		i++
		data = data[:len(data)-1]
	}
	return data, i
}

func ReadBlockRecords(block *Block) {
	records := block.Records
	for i := 0; i < len(records); i++ {
		fmt.Printf("Block Record %d: %v | Size: %d\n", i, records[i], CalculateRecordSize(records[i]))
	}
}

func (bm *BlockManager) PrintBlocks() {
	for i := 0; i < len(bm.Blocks); i++ {
		fmt.Printf("Block ID: %d, Current Capacity: %d/%d", bm.Blocks[i].ID, bm.Blocks[i].CurrentCapacity, bm.Blocks[i].FullCapacity)
		fmt.Printf(", Records: %d\n", len(bm.Blocks[i].Records))
		ReadBlockRecords(bm.Blocks[i])
	}
}