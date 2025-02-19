package wal

import "fmt"

const BLOCK_SIZE = 300

type Block struct {
	ID              int
	Records         []*Record
	FullCapacity    uint64
	CurrentCapacity uint64
}

func NewBlock(BlockID int) *Block {
	return &Block{
		ID:              BlockID,
		Records:         []*Record{},
		FullCapacity:    BLOCK_SIZE,
		CurrentCapacity: 0,
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
			if uint64(recordFullSize) > currentBlock.FullCapacity {
				// make a new Block and use Fragmentation
				return 'u'
			} else {
				// make a new Block and use Padding
				return 'b'
			}
		}
	}
}

func (w *Wal) AddRecordToBlock(record *Record) {
	currentBlock := w.CurrentSegment.Blocks[len(w.CurrentSegment.Blocks) - 1]
	chosenOperation := ChosenOperation(currentBlock, record)

	switch(chosenOperation) {
	case 'n':
		w.HandleZeros(currentBlock, record)
		w.SaveRecordToBlock(currentBlock, record, false)
	case 'p':
		w.HandleZeros(currentBlock, record)
	case 'b':
		newBlockID := currentBlock.ID + 1
		newBlock := NewBlock(newBlockID)
		w.CurrentSegment.Blocks = append(w.CurrentSegment.Blocks, newBlock)
		w.HandleZeros(newBlock, record)
	case 'f':
		w.FragmentRecord(currentBlock, record)
	case 'u':
		newBlockID := currentBlock.ID + 1
		newBlock := NewBlock(newBlockID)
		w.CurrentSegment.Blocks = append(w.CurrentSegment.Blocks, newBlock)
		w.FragmentRecord(newBlock, record)
	}
}

func (w *Wal) SaveRecordToBlock(block *Block, record  *Record, isPadding bool) {
	if isPadding {
		block.Records = append(block.Records, record)
		block.CurrentCapacity += uint64(CalculateRecordSize(record)) - uint64(len(record.Value)) + record.ValueSize
	} else {
		block.Records = append(block.Records, record)
		block.CurrentCapacity += uint64(CalculateRecordSize(record))
	}
	if w.CurrentSegment.IsFull() {
		w.FlushCurrentSegment()
		w.HandleFullness(w.CurrentSegment)
		w.AddNewSegment()
	} else {
		w.FlushCurrentSegment()
	}
}

func (w *Wal) HandleZeros(block *Block, record *Record) {
	if len(block.Records) > 0 {
		lastAddedRecord := block.Records[len(block.Records)-1]
		lastAddedRecord.Value = TrimZeros(lastAddedRecord.Value)
		lastAddedRecord.ValueSize = uint64(len(lastAddedRecord.Value))
	}

	numOfZeros := int64(block.FullCapacity) - int64(CalculateRecordSize(record)) - int64(block.CurrentCapacity) // current capacity is capacity of all records before THIS

	if numOfZeros > 0 {
		padding := make([]byte, numOfZeros)
		record.Value = append(record.Value, padding...)	// zeros are not actual value so increasing ValueSize won't be done
		w.SaveRecordToBlock(block, record, true)
	}
}

func (w *Wal) FragmentRecord (block *Block, record *Record) {
	allButValue := (uint64(CalculateRecordSize(record)) - uint64(len(record.Value)))
	spaceFirst := block.FullCapacity - block.CurrentCapacity - allButValue
	// FIRST
	firstRecord := *record
	firstRecord.Value = make([]byte, spaceFirst) 
	copy(firstRecord.Value, record.Value[:spaceFirst])
	firstRecord.ValueSize = spaceFirst
	firstRecord.Type = 'f'
	w.HandleZeros(block, &firstRecord)
	w.SaveRecordToBlock(block, &firstRecord, false)
	
	remainingValue := record.Value[spaceFirst:]
	remainingSize := uint64(len(remainingValue))

	for remainingSize > 0 {
		// kada se pravi novi segment pravi se novi blok takodje, ovdje kad se fragmentira pravi se opet novi blok i nastavlja se brojac od prethodnog segmenta
		newBlock := NewBlock(block.ID + 1)
		w.CurrentSegment.Blocks = append(w.CurrentSegment.Blocks, newBlock)
		block = newBlock

		// MIDDLE
		spaceMiddle := block.FullCapacity-allButValue
		if remainingSize > spaceMiddle {
			middleRecord := *record
			middleRecord.Value = make([]byte, spaceMiddle)
			copy(middleRecord.Value, remainingValue[:spaceMiddle])
			middleRecord.ValueSize = spaceMiddle
			middleRecord.Type = 'm'
			w.SaveRecordToBlock(block, &middleRecord, false)

			remainingValue = remainingValue[spaceMiddle:]
			remainingSize -= block.FullCapacity - allButValue
		} else {
			// LAST
			lastRecord := *record
			lastRecord.Value = make([]byte,remainingSize)
			copy(lastRecord.Value, remainingValue)
			lastRecord.ValueSize = remainingSize
			lastRecord.Type = 'l'
			w.HandleZeros(block, &lastRecord)
			remainingSize = 0
		}
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
		fmt.Printf("Record %d: %v | Size: %d\n", i, records[i], CalculateRecordSize(records[i]))
	}
}

func (s *Segment) PrintBlocks() {
	for i := 0; i < len(s.Blocks); i++ {
		fmt.Printf("\nBlock ID: %d, Current/Full Capacity: %d/%d", s.Blocks[i].ID, s.Blocks[i].CurrentCapacity, s.Blocks[i].FullCapacity)
		fmt.Printf(", Records: %d\n", len(s.Blocks[i].Records))
		ReadBlockRecords(s.Blocks[i])
	}
}