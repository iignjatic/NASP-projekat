package wal

import "fmt"

type Segment struct {
	ID              int      // Segment ID
	Blocks          []*Block // Array of Blocks
	CurrentCapacity int      // Current number of blocks in a Segment
	FullCapacity    int      // Max number of blocks in a Segment
	FilePath        string   // The path to the file where the segment is stored
}

func NewSegment(ID, fullCapacity int, filePath string) *Segment {
	return &Segment{
		ID:              ID,
		Blocks:          []*Block{},
		CurrentCapacity: 0,
		FullCapacity:    fullCapacity,
		FilePath:        filePath,
	}
}

func (s *Segment) AddBlock(block *Block) {
	if s.CurrentCapacity == s.FullCapacity {
		fmt.Println("Segment is full. Flush the segment to disk before adding new blocks.")
		return
	}

	s.Blocks = append(s.Blocks, block)
	s.CurrentCapacity++
	fmt.Printf("Block with ID %d added to Segment with ID %d.\n", block.ID, s.ID)
}

func (s *Segment) FlushToDisk() {
	// Logic
	fmt.Printf("Flushing Segment %d to file: %s\n", s.ID, s.FilePath)
	// Serialize, write blocks to the disk
	s.Blocks = []*Block{}
	s.CurrentCapacity = 0
}