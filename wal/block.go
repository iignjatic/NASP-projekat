package wal

import (
	"NASP-PROJEKAT/data"
	"fmt"
)

const (
	OpNormal = iota
	OpPadding
	OpFragment
	OpFragmentNewBlock
	OpNewBlockPadding
)

type Block struct {
	ID              int
	Records         []*data.Record
	FullCapacity    uint64
	CurrentCapacity uint64
}

func NewBlock(BlockID int, blockSize uint64) *Block {
	return &Block{
		ID:              BlockID,
		Records:         []*data.Record{},
		FullCapacity:    blockSize,
		CurrentCapacity: 0,
	}
}

func ChosenOperation(currentBlock *Block, record *data.Record) int {
	remaining := currentBlock.FullCapacity - currentBlock.CurrentCapacity
	recordSize := uint64(data.CalculateRecordSize(record))
	baseSize := recordSize - record.ValueSize

	switch {
	case remaining == recordSize:
		return OpNormal
	case remaining > recordSize:
		return OpPadding
	case remaining > baseSize:
		return OpFragment
	case recordSize > currentBlock.FullCapacity:
		return OpFragmentNewBlock
	default:
		return OpNewBlockPadding
	}
}

func (w *Wal) AddRecordToBlock(record *data.Record) {
	block := w.getCurrentBlock()

	switch ChosenOperation(block, record) {
	case OpNormal:
		w.writeNormal(block, record)
	case OpPadding:
		w.writePadding(block, record)
	case OpNewBlockPadding:
		w.newBlockThenPadding(record)
	case OpFragment:
		w.fragmentInSameBlock(block, record)
	case OpFragmentNewBlock:
		w.fragmentInNewBlock(record)
	}
}

func (w *Wal) writeNormal(block *Block, record *data.Record) {
	w.HandleZeros(block, record)
	w.SaveRecordToBlock(block, record, false)
}

func (w *Wal) writePadding(block *Block, record *data.Record) {
	w.HandleZeros(block, record)
}

func (w *Wal) newBlockThenPadding(record *data.Record) {
	block := w.createNextBlock()
	if uint64(data.CalculateRecordSize(record)) == block.FullCapacity {
		w.SaveRecordToBlock(block, record, false)
	} else {
		w.HandleZeros(block, record)
	}
}

func (w *Wal) fragmentInSameBlock(block *Block, record *data.Record) {
	w.FragmentRecord(block, record)
}

func (w *Wal) fragmentInNewBlock(record *data.Record) {
	block := w.createNextBlock()
	w.FragmentRecord(block, record)
}

func (w *Wal) createNextBlock() *Block {
	if !w.CurrentSegment.HasSpaceForNewBlock(w.blocksPerSegment) {
		w.FlushCurrentSegment()
		w.WriteFirstByte(w.CurrentSegment)
		w.AddNewSegment()
		return w.getCurrentBlock()
	}

	lastBlock := w.getCurrentBlock()
	newBlock := NewBlock(lastBlock.ID+1, w.blockSize)
	w.CurrentSegment.Blocks = append(w.CurrentSegment.Blocks, newBlock)
	return newBlock
}

func (w *Wal) getCurrentBlock() *Block {
	return w.CurrentSegment.Blocks[len(w.CurrentSegment.Blocks)-1]
}

func (w *Wal) SaveRecordToBlock(block *Block, record *data.Record, isPadding bool) {
	if isPadding {
		block.Records = append(block.Records, record)
		block.CurrentCapacity += uint64(data.CalculateRecordSize(record)) - uint64(len(record.Value)) + record.ValueSize
	} else {
		block.Records = append(block.Records, record)
		block.CurrentCapacity += uint64(data.CalculateRecordSize(record))
	}

	if w.CurrentSegment.IsFull() {
		w.FlushCurrentSegment()
		w.WriteFirstByte(w.CurrentSegment)
		w.AddNewSegment()
	} else {
		w.FlushCurrentSegment()
	}
}

func (w *Wal) HandleZeros(block *Block, record *data.Record) {
	if len(block.Records) > 0 {
		last := block.Records[len(block.Records)-1]
		last.Value = TrimZeros(last.Value)
		last.ValueSize = uint64(len(last.Value))
	}

	numZeros := int64(block.FullCapacity) - int64(data.CalculateRecordSize(record)) - int64(block.CurrentCapacity)
	if numZeros > 0 {
		padding := make([]byte, numZeros)
		record.Value = append(record.Value, padding...)
		w.SaveRecordToBlock(block, record, true)
	}
}

func (w *Wal) FragmentRecord(block *Block, record *data.Record) {
	allButValue := (uint64(data.CalculateRecordSize(record)) - uint64(len(record.Value)))
	spaceFirst := block.FullCapacity - block.CurrentCapacity - allButValue

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
		lastBlock := w.getCurrentBlock()
		if lastBlock == block {
			block = w.createNextBlock()
		} else {
			block = lastBlock
		}

		spaceMiddle := block.FullCapacity - allButValue
		if remainingSize > spaceMiddle {
			midRecord := *record
			midRecord.Value = make([]byte, spaceMiddle)
			copy(midRecord.Value, remainingValue[:spaceMiddle])
			midRecord.ValueSize = spaceMiddle
			midRecord.Type = 'm'
			w.SaveRecordToBlock(block, &midRecord, false)

			remainingValue = remainingValue[spaceMiddle:]
			remainingSize -= spaceMiddle
		} else {
			lastRecord := *record
			lastRecord.Value = make([]byte, remainingSize)
			copy(lastRecord.Value, remainingValue)
			lastRecord.ValueSize = remainingSize
			lastRecord.Type = 'l'
			w.HandleZeros(block, &lastRecord)
			remainingSize = 0
		}
	}
}

func TrimZeros(data []byte) []byte {
	for len(data) > 0 && data[len(data)-1] == 0 {
		data = data[:len(data)-1]
	}
	return data
}

func ReadBlockRecords(block *Block) {
	for i, rec := range block.Records {
		fmt.Printf("Record %d: %v | Size: %d\n", i, rec, data.CalculateRecordSize(rec))
	}
}

func (s *Segment) PrintBlocks() {
	for _, block := range s.Blocks {
		fmt.Printf("\nBlock ID: %d, Current/Full Capacity: %d/%d", block.ID, block.CurrentCapacity, block.FullCapacity)
		fmt.Printf(", Records: %d\n", len(block.Records))
		ReadBlockRecords(block)
	}
}
