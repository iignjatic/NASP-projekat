package wal

import "fmt"

const (
	BLOCKS_PER_SEGMENT = 4
	SEGMENT_SIZE = BLOCKS_PER_SEGMENT * BLOCK_SIZE
)

type Segment struct {
	ID           int
	Blocks       []*Block
	FullCapacity uint64
	Transferred	 bool
	FilePath     string
	Offset		 uint64
}

func NewSegment(id int) *Segment {
	firstBlock := NewBlock(0)
	segment := &Segment{
		ID:           id,
		Blocks:       []*Block{firstBlock},
		FullCapacity: SEGMENT_SIZE,
		Transferred:  false,
		Offset: 	  0,
	}
	segment.FilePath = fmt.Sprintf("wal_%d.bin", segment.ID)
	return segment
}

func (s *Segment) IsFull() bool {
	usedCapacity := uint64(0)
	for i:=0;i<len(s.Blocks);i++ {
		if len(s.Blocks[i].Records) > 0 {
			usedCapacity += s.Blocks[i].CurrentCapacity + s.BackZeros(i)
		}
	}
	// println(usedCapacity)
	return usedCapacity >= s.FullCapacity
}

// full capacity works based on real values, not zeros in the end
// if zeros in the end of record are padding(strict number of zeros), there cannot be written another record - number of that zeros counts as capacity
// that zeros can appear only in the last records of blocks
func (s *Segment) BackZeros(i int) uint64 {
	lastRecord := s.Blocks[i].Records[len(s.Blocks[i].Records) - 1]
	data := lastRecord.Value
	zerosCount := 0
	for len(data) > 0 && data[len(data)-1] == 0 {
		zerosCount++
		data = data[:len(data)-1]
	}

	if zerosCount == 0 {
		// zero zeros in the end of the block - full capacity used
		return 0
	} else {
		// one more record can be written - do not count
		if zerosCount > CalculateRecordSize(lastRecord) - len(lastRecord.Value) {
			return 0
		} else {
			// no record can be written in the end of the block
			return uint64(zerosCount)
		}
	}
}