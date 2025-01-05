package SSTable

import (
	"os"
)

type BlockManager struct {
}

func (blockManager *BlockManager) writeBlock(block *Block, filePath string, numberOfBlock uint32) {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 077)

	if err != nil {
		panic(err)
	}

	file.Seek(int64(numberOfBlock)*int64(block.BlockSize), 0)
	file.Write(block.records)

	defer file.Close()
}
