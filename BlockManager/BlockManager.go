package BlockManager

import (
	"NASP-PROJEKAT/BlockCache"
	"NASP-PROJEKAT/data"
	"fmt"
	"io"
	"os"
	"strconv"
)

type BlockManager struct {
	BlockCache BlockCache.BlockCache
	BlockSize  uint64
}

//func (blockManager *BlockManager) writeBlock(block *Block, filePath string, numberOfBlock uint32) {
// file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 077)

// if err != nil {
// 	panic(err)
// }

// file.Seek(int64(numberOfBlock)*int64(block.BlockSize), 0)
// file.Write(block.records)

// defer file.Close()

// }
func (blockManager *BlockManager) WriteBlock(block *data.Block, filePath string, numberOfBlock uint64, BlockSize uint64, metaSummary uint64) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	//kreiram niz tacne velicine BlockSize
	blockWithPadding := make([]byte, BlockSize)

	//kopiram postojece podatke i time postizem da je ostatak bloka popunjen nulama
	//jer pri poslednjim zapisima se desi da blok ostane nepopunjen do kraja
	copy(blockWithPadding, block.Records)
	offset := int64(numberOfBlock)*int64(BlockSize) + int64(metaSummary)
	_, err = file.WriteAt(blockWithPadding, offset)
	if err != nil {
		panic(err)
	}
}

func (BlockManager *BlockManager) ReadBlock(filePath string, numberOfBlock uint64, indicator byte, metaSummary int64) ([]byte, error) {
	var buffer []byte
	if BlockManager.BlockCache.CheckCache(strconv.Itoa(int(numberOfBlock))+filePath) != nil {
		buffer = BlockManager.BlockCache.BlockMap[strconv.Itoa(int(numberOfBlock))+filePath].Block.Records
	} else {
		file, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
		if err != nil {
			return nil, err
		}
		defer file.Close()

		buffer = make([]byte, BlockManager.BlockSize) //u bafer ce biti ucitani podaci koje vraca funkcija
		offset := int64(numberOfBlock)*int64(BlockManager.BlockSize) + int64(metaSummary)
		_, err = file.ReadAt(buffer, offset)
		//readAt cita onoliko bajtova koliko moze da stane u bafer a to je velicina jednog bloka
		//cita sa pozicije u datoteci koja je drugi parametar funkcije
		if err != nil {
			return nil, err
		}
		block := &data.Block{
			Records: buffer,
		}
		BlockManager.BlockCache.AddCache(strconv.Itoa(int(numberOfBlock))+filePath, block)
	}
	return buffer, nil
}

func (bm *BlockManager) WriteIndicatorByte(filePath string, indicator byte) error {
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.WriteAt([]byte{indicator}, 0)
	return err
}

func (bm *BlockManager) ReadIndicatorByte(filePath string) (byte, error) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	buffer := make([]byte, 1)
	_, err = file.Read(buffer)
	if err != nil {
		return 0, err
	}
	return buffer[0], nil
}

func (blockManager *BlockManager) ReadWalBlock(filePath string, numberOfBlock uint64, indicatorSize int64) ([]byte, error) {
	buffer := make([]byte, blockManager.BlockSize)
	offset := int64(numberOfBlock)*int64(blockManager.BlockSize) + indicatorSize
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()
	bytesRead, err := file.ReadAt(buffer, offset)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read block: %w", err)
	}
	if bytesRead == 0 {
		return nil, io.EOF
	}
	return buffer[:bytesRead], nil
}