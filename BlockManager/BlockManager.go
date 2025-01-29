package BlockManager

import (
	"NASP-PROJEKAT/BlockCache"
	"NASP-PROJEKAT/data"
	"os"
	"strconv"
)

type BlockManager struct {
	BlockCache BlockCache.BlockCache
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
	if BlockManager.BlockCache.CheckCache(strconv.Itoa(int(numberOfBlock))+string(indicator)) != nil {
		buffer = BlockManager.BlockCache.BlockMap[strconv.Itoa(int(numberOfBlock))+string(indicator)].Block.Records
	} else {
		file, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
		if err != nil {
			return nil, err
		}
		defer file.Close()

		buffer = make([]byte, data.BlockSize) //u bafer ce biti ucitani podaci koje vraca funkcija
		offset := int64(numberOfBlock)*int64(data.BlockSize) + int64(metaSummary)
		_, err = file.ReadAt(buffer, offset)
		//readAt cita onoliko bajtova koliko moze da stane u bafer a to je velicina jednog bloka
		//cita sa pozicije u datoteci koja je drugi parametar funkcije
		if err != nil {
			return nil, err
		}
		block := &data.Block{
			Records: buffer,
		}
		BlockManager.BlockCache.AddCache(strconv.Itoa(int(numberOfBlock))+string(indicator), block)
	}
	return buffer, nil
}
