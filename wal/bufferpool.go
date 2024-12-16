package wal

import "fmt"

type BufferPool struct {
	FullCapacity    int         // Max number of blocks in a Buffer Pool
	CurrentCapacity int         // Current number of blocks in a Buffer Pool
	Blocks          []*Block    // Array of blocks
}

// Initialize Buffer Pool
func NewBufferPool(FullCapacity int) *BufferPool {
	return &BufferPool{
		FullCapacity:    FullCapacity,
		CurrentCapacity: 0,
		Blocks:          []*Block{},
	}
}

func (bp *BufferPool) AddBlock(blockID int, records []*Record) {
	if bp.CurrentCapacity == bp.FullCapacity {
		bp.FlushToDisk()
	}
	
	newBlock := NewBlock(blockID, BlockSize, records)

	bp.Blocks = append(bp.Blocks, &newBlock)
	bp.CurrentCapacity++;
	fmt.Printf("Block with id %d added in Buffer Pool.\n", blockID)
}

func (bp *BufferPool) FlushToDisk() {
	if len(bp.Blocks) == 0 {
		fmt.Println("Buffer Pool is already empty, nothing to flush.")
		return 
	}
	fmt.Println("Flushing all blocks to disk (WAL):")
	for _, block := range bp.Blocks {
		fmt.Printf("Flushing block ID: %d (Full Capacity: %d)\n", block.ID, block.FullCapacity)
		fmt.Printf("Block records length: %d\n", len(block.Records))
	}

	// Clear blocks after flushing
	bp.Blocks = []*Block{}
	bp.CurrentCapacity = 0
	fmt.Println("Buffer Pool is now empty.")
}