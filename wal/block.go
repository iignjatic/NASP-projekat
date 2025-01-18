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

// before adding a new Record to the Block, it have to be decided which operation should be performed
func ChosenOperation(currentBlock *Block, record *Record) byte {
	remainingBlockCapacity := currentBlock.FullCapacity - currentBlock.CurrentCapacity
	recordFullSize := CalculateRecordSize(record)

	if remainingBlockCapacity == uint64(recordFullSize) {
		// a Record fits in the Block
		return 'n'
	} else if remainingBlockCapacity > uint64(recordFullSize) {
		// padding
		return 'p'
	} else {
		if remainingBlockCapacity > (uint64(recordFullSize) - record.ValueSize) {
			// fragment the Record
			return 'f'
		} else {
			// make a new Block and use Padding
			return 'b'
		}
	}
}

func (bm *BlockManager) AddRecordToBlock(record *Record) {
	currentBlock := bm.Blocks[len(bm.Blocks) - 1]
	chosenOperation := ChosenOperation(currentBlock, record)

	switch(chosenOperation) {
	case 'n':
		SaveRecordToBlock(currentBlock, record, false)
	case 'p':
		HandleZeros(currentBlock, record)
	case 'b':
		newBlockID := currentBlock.ID + 1
		newBlock := NewBlock(newBlockID)
		bm.Blocks = append(bm.Blocks, newBlock)
		HandleZeros(newBlock, record)
	}
}

func SaveRecordToBlock(block *Block, record  *Record, isPadding bool) {
	if isPadding {
		block.Records = append(block.Records, record)
		block.CurrentCapacity += uint64(CalculateRecordSize(record)) - uint64(len(record.Value)) + record.ValueSize
	} else {
		block.Records = append(block.Records, record)
		block.CurrentCapacity += uint64(CalculateRecordSize(record))
	}
}

func HandleZeros(block *Block, record *Record) {
	if len(block.Records) > 0 {
		lastAddedRecord := block.Records[len(block.Records)-1]
		lastAddedRecord.Value = TrimZeros(lastAddedRecord.Value)
		lastAddedRecord.ValueSize = uint64(len(lastAddedRecord.Value))
	}

	numOfZeros := block.FullCapacity - uint64(CalculateRecordSize(record)) - block.CurrentCapacity // current capacity is capacity of all records before THIS
	if numOfZeros > 0 {
		padding := make([]byte, numOfZeros)
		record.Value = append(record.Value, padding...)	// zeros are not actual value so increasing ValueSize won't be done
		SaveRecordToBlock(block, record, true)
	}
}

func TrimZeros(data []byte) ([]byte) {
	for len(data) > 0 && data[len(data)-1] == 0 {
		data = data[:len(data)-1]
	}
	return data
}

func ReadBlockRecords(block *Block) {
	records := block.Records
	for i := 0; i < len(records); i++ {
		fmt.Printf("Block Record %d: %v | Size: %d\n", i, records[i], CalculateRecordSize(records[i]))
	}
}

func (bm *BlockManager) PrintBlocks() {
	for i := 0; i < len(bm.Blocks); i++ {
		fmt.Printf("\nBlock ID: %d, Current Capacity: %d/%d", bm.Blocks[i].ID, bm.Blocks[i].CurrentCapacity, bm.Blocks[i].FullCapacity)
		fmt.Printf(", Records: %d\n", len(bm.Blocks[i].Records))
		ReadBlockRecords(bm.Blocks[i])
	}
}