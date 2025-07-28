package wal

import (
	"fmt"
)

type Segment struct {
	ID           int
	Blocks       []*Block
	FullCapacity uint64
	FilePath     string
}

func NewSegment(id int, blockSize, blocksPerSegment uint64) *Segment {
	segment := &Segment{
		ID:           id,
		Blocks:       []*Block{},
		FullCapacity: blockSize*blocksPerSegment,
	}
	segment.FilePath = fmt.Sprintf("wal_%d.bin", segment.ID)
	return segment
}

func (s *Segment) HasSpaceForNewBlock(blocksPerSegment uint64) bool {
	return len(s.Blocks) < int(blocksPerSegment)
}

func (s *Segment) IsFull() bool {
	usedCapacity := uint64(0)
	for _, block := range s.Blocks {
		usedCapacity += block.CurrentCapacity
	}
	return usedCapacity >= s.FullCapacity
}