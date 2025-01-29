package data

// segmentSize - broj blokova
// blockSize - broj zapisa u bloku

const BlockSize uint64 = 70 //velicina bloka je 32 kilobajta

type Block struct {
	Records []byte
}
