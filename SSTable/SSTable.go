package SSTable

import (
	"NASP-PROJEKAT/BlockManager"
	"NASP-PROJEKAT/data"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

type SSTable struct {
	DataSegment     *DataSegment
	Index           *Index
	Summary         *Summary
	BlockManager    *BlockManager.BlockManager
	DataFilePath    string
	IndexFilePath   string
	SummaryFilePath string
	BlockSize       uint64
}

func (sst *SSTable) ReadMeta() { //podaci o prvom i poslednjem kljucu
	file, err := os.Open(sst.SummaryFilePath)
	if err != nil {
		panic(err)
	}
	bytes := make([]byte, 8)
	_, err = file.Read(bytes)
	firstKeySize := binary.LittleEndian.Uint64(bytes)
	file.Seek(8, 0)
	bytes = make([]byte, firstKeySize)
	_, err = file.Read(bytes)
	firstKey := string(bytes)
	sst.Summary.First = firstKey
	file.Seek(8+int64(firstKeySize), 0)
	bytes = make([]byte, 8)
	_, err = file.Read(bytes)
	lastKeySize := binary.LittleEndian.Uint64(bytes)
	file.Seek(int64(data.KEY_SIZE+data.KEY_SIZE+firstKeySize), 0)
	bytes = make([]byte, lastKeySize)
	_, err = file.Read(bytes)
	lastKey := string(bytes)
	sst.Summary.Last = lastKey
	sst.Summary.Meta = 2*data.KEY_SIZE + firstKeySize + lastKeySize

}

// vraca velicinu niza rekorda data segmenta u bajtovima
func (sst *SSTable) getDataSize(records []*data.Record) uint64 {
	var totalSize uint64 = 0
	for i := 0; i < len(records); i++ {
		recordSize := sst.DataSegment.GetRecordSize(records[i])
		totalSize += recordSize
	}
	return totalSize
}

// vraca velicinu niza rekorda indeksa  u bajtovima
func (sst *SSTable) getIndexSize(records []*data.Record) uint64 {
	var totalSize uint64 = 0
	for i := 0; i < len(records); i++ {
		recordSize := sst.Index.getRecordSize(records[i])
		totalSize += recordSize
	}
	return totalSize
}

// vraca velicinu niza rekorda samarija  u bajtovima
func (sst *SSTable) getSummarySize(records []*data.Record) uint64 {
	var totalSize uint64 = 0
	var summaryCount = 0
	for i := 0; i < len(records); i++ { //ako je npr  10 (sample) zapisa proslo tek tad azuriramo brojac
		if summaryCount == int(sst.Summary.Sample) {
			recordSize := sst.Index.getRecordSize(records[i])
			totalSize += recordSize
			summaryCount = 0
		}
		summaryCount++
	}
	return totalSize
}

func (sstable *SSTable) MakeSSTable(records []*data.Record) {
	//blokovi za data segment
	DataSize := sstable.getDataSize(records)                                       //ukupna velicina data dijela
	sstable.DataSegment.Blocks = make([]*data.Block, DataSize/sstable.BlockSize+1) //bilo DataSize/BlockSize*2
	for i := range sstable.DataSegment.Blocks {
		sstable.DataSegment.Blocks[i] = &data.Block{} // Inicijalizacija svakog bloka
	}

	//blokovi za indeks
	IndexSize := sstable.getIndexSize(records)                                //ukupna velicina indeksa
	sstable.Index.Blocks = make([]*data.Block, IndexSize/sstable.BlockSize+1) //bilo IndexSize/BlockSize*2
	for i := range sstable.Index.Blocks {
		sstable.Index.Blocks[i] = &data.Block{} // Inicijalizacija svakog bloka
	}

	//blokovi za summary
	//sstable.Summary.Sample = 2 //OVDJE TREBA SAMPLE !!!!!!

	SummarySize := sstable.getSummarySize(records)                                //ukupna velicina samarija
	sstable.Summary.Blocks = make([]*data.Block, SummarySize/sstable.BlockSize+1) //bilo SummarySize/BlockSize*2
	for i := range sstable.Summary.Blocks {
		sstable.Summary.Blocks[i] = &data.Block{} // Inicijalizacija svakog bloka
	}

	sstable.DataSegment.SegmentSize = uint64(len(sstable.DataSegment.Blocks))
	sstable.Index.SegmentSize = uint64(len(sstable.Index.Blocks))
	sstable.Summary.SegmentSize = uint64(len(sstable.Summary.Blocks))

	if len(records) > 0 {
		sstable.Summary.First = records[0].Key
		sstable.Summary.Last = records[len(records)-1].Key
	}
	firstKeySize := make([]byte, 8)
	lastKeySize := make([]byte, 8)
	firstKey := []byte(records[0].Key)
	binary.LittleEndian.PutUint64(firstKeySize[0:], records[0].KeySize)
	lastKey := []byte(records[len(records)-1].Key)
	binary.LittleEndian.PutUint64(lastKeySize[0:], records[len(records)-1].KeySize)
	err := os.MkdirAll(filepath.Dir(sstable.SummaryFilePath), 0755) //ovdje je bila greska da ne postoji dir
	if err != nil {
		panic(err)
	}

	file, err := os.OpenFile(sstable.SummaryFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		panic(err)
	}

	var meta []byte
	meta = append(meta, firstKeySize...)
	meta = append(meta, firstKey...)
	meta = append(meta, lastKeySize...)
	meta = append(meta, lastKey...)
	file.WriteAt(meta, 0)
	metasize := records[0].KeySize + records[len(records)-1].KeySize + 2*data.KEY_SIZE
	sstable.Summary.Meta = metasize

	sstable.MakeBlocks('d', records)
	sstable.MakeBlocks('i', records)
	sstable.MakeBlocks('s', records)

}

func (sst *SSTable) WriteSSTable() {
	var i uint64 = 0

	for i = 0; i < sst.DataSegment.SegmentSize; i++ {
		sst.BlockManager.WriteBlock(sst.DataSegment.Blocks[i], sst.DataFilePath, i, sst.BlockSize, 0)
	}
	for i = 0; i < sst.Index.SegmentSize; i++ {
		sst.BlockManager.WriteBlock(sst.Index.Blocks[i], sst.IndexFilePath, i, sst.BlockSize, 0)
	}

	for i = 0; i < sst.Summary.SegmentSize; i++ {
		sst.BlockManager.WriteBlock(sst.Summary.Blocks[i], sst.SummaryFilePath, i, sst.BlockSize, uint64(sst.Summary.Meta))
	}

}

// t je tip blokova, za indeks, data ili summary
func (sst *SSTable) MakeBlocks(t byte, records []*data.Record) {
	i := 0 //rekord
	var pos uint64
	var indicator byte = 'a'
	var offsetIndex uint64 = 0
	var offsetSummary uint64 = 0
	var summaryCount int32 = -1
	tempBlockSize := sst.BlockSize
	var recordBytes []byte
	var blocks []*data.Block
	var summaryIndicator = 0 //sluzi za povecanje summaryCount u situacijama kad se zapis prelama
	var indexIndicator = 0   //sluzi za povecanje indexOffseta u situacijama kad se zapis prelama

	if t == 'd' {
		blocks = sst.DataSegment.Blocks
	} else if t == 'i' {
		blocks = sst.Index.Blocks
	} else {
		blocks = sst.Summary.Blocks

	}

	for indexOfBlock := 0; indexOfBlock < len(blocks); indexOfBlock++ {
		if i >= len(records) { //upisali smo sve rekorde
			break
		}

		for tempBlockSize >= 0 && i < len(records) { //prolazak kroz jedan blok
			recordSize := sst.DataSegment.GetRecordSize(records[i])
			allRecordSize := recordSize

			if t == 'd' { //d data segment
				recordBytes = sst.DataSegment.RecordToBytes(records[i], recordSize, indicator)
			} else if t == 'i' { //i indeks
				recordSize = sst.Index.getRecordSize(records[i]) //azurira se na velicinu zapisa u indeksu
				if indexIndicator == 0 {
					recordBytes = sst.Index.recordToBytes(records[i], recordSize, offsetIndex) //zapis indeksa
					offsetIndex += allRecordSize
				}

			} else { //summary
				recordSize = sst.Index.getRecordSize(records[i])
				if summaryIndicator == 0 { //da se ne bi desilo povecanje u slucaju jednog zapisa koji se prelama
					summaryCount++
				}

				if summaryCount == int32(sst.Summary.Sample) || summaryCount == 0 { //uzorak summaryja
					if summaryIndicator == 0 {
						recordBytes = sst.Index.recordToBytes(records[i], recordSize, offsetSummary)
						offsetSummary += recordSize
						summaryCount = 0
					}

				} else {
					i += 1
					offsetSummary += recordSize
					continue
				}
			}
			if recordSize < tempBlockSize && indicator != 'm' && indicator != 'l' { //ako moze cijeli stati odmah
				indicator = 'a' //all kao citav rekord je stao
				blocks[indexOfBlock].Records = append(blocks[indexOfBlock].Records, recordBytes...)
				tempBlockSize -= recordSize //smanjimo velicinu bloka za velicinu unijetog rekorda
				i += 1                      //prelazak na sledeci rekord
				summaryIndicator = 0
				indexIndicator = 0

			} else if recordSize > tempBlockSize && indicator != 'm' { //gigant je
				//if recordSize > BlockSize && tempBlockSize == BlockSize { //ako zapis ne moze stati u prazan blok
				summaryIndicator = 1 //znak da je u sledecem bloku dio prethodnog zapisa i
				// da se summaryCount ne treba povecati
				indexIndicator = 1
				//tempBlockSize = BlockSize
				indicator = 'f' //first
				//recordBytes := recordToBytes(records[i], recordSize, indicator)
				blocks[indexOfBlock].Records = append(blocks[indexOfBlock].Records, recordBytes[0:tempBlockSize]...)
				//recordSize -= uint32(tempBlockSize)
				pos = tempBlockSize
				tempBlockSize = sst.BlockSize //jer je blok popunjen do kraja
				indicator = 'm'
				break
				//} else { //ako zapis ne moze stati u blok koji vec ima nesto u sebi, popunjavamo ostatak nulama
				// if t == 'd' { NE KORISTI SE
				// 	recordSize = sst.DataSegment.GetRecordSize(records[i])
				// } else {
				// 	recordSize = sst.Index.getRecordSize(records[i])
				// }
				// summaryIndicator = 1
				// indexIndicator = 1
				// //recordBytes = recordToBytes(records[i], recordSize, indicator)
				// padding := make([]byte, tempBlockSize ) //preostali prostor se popuni paddingom
				// blocks[indexOfBlock].records = append(blocks[indexOfBlock].records, padding...)

				// tempBlockSize = BlockSize
				// break
				//	}

			} else if indicator == 'm' { //middle gigant
				//recordSize = recordSize - BlockSize
				recordSize = recordSize - uint64(pos)
				if recordSize < sst.BlockSize { //ako ostatak zapisa moze stati u taj blok to je poslednji dio zapisa l''
					indicator = 'l'
					//recordBytes := recordToBytes(records[i], recordSize, indicator)
					blocks[indexOfBlock].Records = append(blocks[indexOfBlock].Records, recordBytes[pos:]...)
					i += 1
					summaryIndicator = 0
					indexIndicator = 0
					indicator = 'a' //resetovanje indikatora
					tempBlockSize -= recordSize
					if tempBlockSize == 0 {
						tempBlockSize = sst.BlockSize
					}
					pos += recordSize
					//padding := make([]byte, BlockSize-recordSize) //preostali prostor se popuni paddingom
					//blocks[indexOfBlock].records = append(blocks[indexOfBlock].records, padding...)

					//tempBlockSize = BlockSize
					//break //prelazak na sledeci blok jer je trenutni popunjen nulama do kraja

				} else { //ako ne moze ostaje indikator 'm' jer jos nismo dosli do kraja zapisa
					//recordBytes := recordToBytes(records[i], recordSize, indicator)
					summaryIndicator = 1
					indexIndicator = 1
					blocks[indexOfBlock].Records = append(blocks[indexOfBlock].Records, recordBytes[pos:sst.BlockSize+pos]...)
					//bilo ...append(blocks[indexOfBlock].records, recordBytes[pos:BlockSize]...)
					pos = pos + sst.BlockSize
					tempBlockSize = sst.BlockSize
					break //prelazimo na sledeci blok
				}

			} else if recordSize == sst.BlockSize && indicator != 'm' { //OVO SE NIKAD NE DESAVA
				indicator = 'a'
				if t == 'd' {
					recordBytes = sst.DataSegment.RecordToBytes(records[i], recordSize, indicator)
				} else if t == 'i' {
					recordBytes = sst.Index.recordToBytes(records[i], recordSize, offsetIndex)
				} else {
					recordBytes = sst.Index.recordToBytes(records[i], recordSize, offsetSummary)
				}
				blocks[indexOfBlock].Records = append(blocks[indexOfBlock].Records, recordBytes...)
				tempBlockSize = sst.BlockSize
				i += 1

			} else {
				break
			}
		}
	}
}

func (sst *SSTable) SearchSummary(key string) int64 {
	// if key < sst.Summary.First || key > sst.Summary.Last {
	// 	return -1
	// }
	sst.ReadMeta()
	var currentBlock uint64 = 0
	var lastSmallerOffset int64 = -1
	readIndicator := -1   //koristi se za identifikovnje da li se cita keySize(0), key(1), offset(2), pocetno stanje(-1)
	var helpBuffer []byte //koristi se za bajtove zapisa koji se prelamaju u sledeci blok
	var keySize uint64
	totalSize := 0
	var summaryKey string
	var helpKeySize uint64 = 0

	for currentBlock < sst.Summary.SegmentSize { //prolazak kroz blokove
		buffer, err := sst.BlockManager.ReadBlock(sst.SummaryFilePath, currentBlock, 's', int64(sst.Summary.Meta))
		if err != nil {
			return -1
		}

		pos := 0

		for pos < len(buffer) { //prolazak kroz jedan blok
			// obrada nepotpunog zapisa iz prethodnog bloka
			if readIndicator == 0 { //znaci da se u baferu nalazi ostatak keySize
				// citamo keySize
				remainingSize := data.KEY_SIZE - len(helpBuffer) //ostatak keySize koji je u trenutnom bloku
				//dodam preostali dio keySize i pomjerim se u bloku
				helpBuffer = append(helpBuffer, buffer[:remainingSize]...)
				pos += remainingSize //sad sam na poziciji na kojoj krece key
				//a u pomocnom baferu je cijeli keySize
				helpKeySize = binary.LittleEndian.Uint64(helpBuffer)
				helpBuffer = nil

			} else if readIndicator == 2 { // u pomocnom baferu je ostatak offseta
				remainingSize := 8 - len(helpBuffer) //velicina ostatka offseta
				helpBuffer = append(helpBuffer, buffer[:remainingSize]...)
				pos += remainingSize
				offset := binary.LittleEndian.Uint64(helpBuffer)
				if summaryKey > key {
					return lastSmallerOffset
				} else if summaryKey == key {
					return int64(offset)
				}
				lastSmallerOffset = int64(offset)
				helpBuffer = nil
				readIndicator = -1 //prelazimo na sledeci zapis
			} else if readIndicator == 1 {
				remainingSize := int64(keySize) - int64(len(helpBuffer)) //velicina ostatka kljuca
				//dodam preostali dio key i pomjerim se u bloku
				helpBuffer = append(helpBuffer, buffer[:remainingSize]...)
				pos += int(remainingSize)

				summaryKey = string(helpBuffer)
				helpBuffer = nil
				readIndicator = 2 //oznaka za citanje offseta

			}

			// citanje novog zapisa
			if pos+data.KEY_SIZE > (len(buffer)) && readIndicator == -1 {
				// nemamo dovoljno za keySize
				helpBuffer = append(helpBuffer, buffer[pos:]...)
				readIndicator = 0 //indikator da je u pomocnom baferu keySize
				break
			}
			if readIndicator != 2 {
				if helpKeySize != 0 { //ako je keySize vec procitan
					keySize = helpKeySize
				} else {
					keySize = binary.LittleEndian.Uint64(buffer[pos : pos+data.KEY_SIZE])
				}
				if keySize == 0 { //VIDI JE LI NEOPHODNO
					//prelazim u sledeci blok jer je 0 znak da je do kraja tog bloka padding
					break
				}

				totalSize = data.KEY_SIZE + int(keySize) + 8
				if pos+data.KEY_SIZE+int(keySize) > (len(buffer)) { //ako ne moze da se procita citav key
					helpBuffer = append(helpBuffer, buffer[pos+data.KEY_SIZE:]...)
					readIndicator = 1
					break
				}
				//ako key moze da se procita iz tog bloka
				summaryKey = string(buffer[pos+data.KEY_SIZE : pos+data.KEY_SIZE+int(keySize)])
				if pos+data.KEY_SIZE+int(keySize)+8 > (len(buffer)) { //ako ne moze da se procita citav offset
					helpBuffer = append(helpBuffer, buffer[pos+data.KEY_SIZE+int(keySize):]...)
					readIndicator = 2
					break
				}
				//ako offset moze da se procita iz tog bloka
				offset := binary.LittleEndian.Uint64(buffer[pos+data.KEY_SIZE+int(keySize) : pos+data.KEY_SIZE+int(keySize)+8])

				if summaryKey > key {
					return lastSmallerOffset
				} else if summaryKey == key {
					return int64(offset)
				}
				lastSmallerOffset = int64(offset)
				pos += int(totalSize)
				readIndicator = -1

			} else if readIndicator == 2 {
				if pos+8 > (len(buffer)) {
					break
				}
				offset := binary.LittleEndian.Uint64(buffer[pos : pos+8])

				if summaryKey > key {
					return lastSmallerOffset
				} else if summaryKey == key {
					return int64(offset)
				}
				lastSmallerOffset = int64(offset)
				pos += 8
				readIndicator = -1

			}

		}
		currentBlock++
	}
	return lastSmallerOffset
}

func (sst *SSTable) SearchIndex(key string, offset int64) int64 {
	startBlock := offset / int64(sst.BlockSize) //racunanje bloka od kog krece pretraga na osnovu offseta
	// if offset%int32(BlockSize) != 0 {
	// 	startBlock++
	// }

	currentBlock := startBlock
	var errorOffset int64 = -1 //povratna vrijednost funkcije u slucaju da se kljuc ne nalazi u indeksu
	readIndicator := -1        //koristi se za identifikovnje da li se cita keySize(0), key(1), offset(2), pocetno stanje(-1)
	var helpBuffer []byte      //koristi se za bajtove zapisa koji se prelamaju u sledeci blok
	var keySize uint64
	totalSize := 0
	var summaryKey string
	var helpKeySize uint64 = 0

	for currentBlock < int64(sst.Index.SegmentSize) { //prolazak kroz blokove indexa
		buffer, err := sst.BlockManager.ReadBlock(sst.IndexFilePath, uint64(currentBlock), 'i', 0)
		if err != nil {
			return -1
		}
		// racunamo pocetnu poziciju samo za prvi blok, za svaki sledeci pocinjemo od nulte pozicije
		pos := int64(0)
		if currentBlock == startBlock {
			pos = offset % int64(sst.BlockSize)
		}

		for pos < int64(len(buffer)) { //prolazak kroz jedan blok
			// obrada nepotpunog zapisa iz prethodnog bloka
			if readIndicator == 0 { //znaci da se u baferu nalazi ostatak keySize
				// citamo keySize
				remainingSize := data.KEY_SIZE - len(helpBuffer) //ostatak keySize koji je u trenutnom bloku
				//dodam preostali dio keySize i pomjerim se u bloku
				helpBuffer = append(helpBuffer, buffer[:remainingSize]...)
				pos += int64(remainingSize) //sad sam na poziciji na kojoj krece key
				//a u pomocnom baferu je cijeli keySize
				helpKeySize = binary.LittleEndian.Uint64(helpBuffer)
				helpBuffer = nil
				//readIndicator = 1 //prelazim na citanje key i offset
				//continue
			} else if readIndicator == 2 { // u pomocnom baferu je ostatak offseta
				remainingSize := 8 - len(helpBuffer) //velicina ostatka offseta
				helpBuffer = append(helpBuffer, buffer[:remainingSize]...)
				pos += int64(remainingSize)
				offset := binary.LittleEndian.Uint64(helpBuffer)
				if summaryKey == key {
					return int64(offset)
				}
				//lastSmallerOffset = int32(offset)
				helpBuffer = nil
				readIndicator = -1 //prelazimo na sledeci zapis
			} else if readIndicator == 1 {
				remainingSize := int(keySize) - len(helpBuffer) //velicina ostatka kljuca
				//dodam preostali dio key i pomjerim se u bloku
				helpBuffer = append(helpBuffer, buffer[:remainingSize]...)
				pos += int64(remainingSize)

				summaryKey = string(helpBuffer)
				helpBuffer = nil
				readIndicator = 2 //oznaka za citanje offseta
				//continue

			}

			// citanje novog zapisa
			if pos+data.KEY_SIZE > int64(len(buffer)) && readIndicator == -1 {
				// nemamo dovoljno za keySize
				helpBuffer = append(helpBuffer, buffer[pos:]...)
				readIndicator = 0 //indikator da je u pomocnom baferu keySize
				break
			}
			if readIndicator != 2 {
				if helpKeySize != 0 { //ako je keySize vec procitan
					keySize = helpKeySize
				} else {
					keySize = binary.LittleEndian.Uint64(buffer[pos : pos+data.KEY_SIZE])
				}
				if keySize == 0 { //VIDI JE LI NEOPHODNO
					break
				}

				totalSize = data.KEY_SIZE + int(keySize) + 8
				if pos+data.KEY_SIZE+int64(keySize) > int64(len(buffer)) { //ako ne moze da se procita citav key
					helpBuffer = append(helpBuffer, buffer[pos+data.KEY_SIZE:]...)
					readIndicator = 1
					break
				}
				//ako key moze da se procita iz tog bloka
				summaryKey = string(buffer[pos+data.KEY_SIZE : pos+data.KEY_SIZE+int64(keySize)])
				if pos+data.KEY_SIZE+int64(keySize)+8 > int64(len(buffer)) { //ako ne moze da se procita citav offset
					helpBuffer = append(helpBuffer, buffer[pos+data.KEY_SIZE+int64(keySize):]...)
					readIndicator = 2
					break
				}
				//ako offset moze da se procita iz tog bloka
				offset := binary.LittleEndian.Uint64(buffer[pos+data.KEY_SIZE+int64(keySize) : pos+data.KEY_SIZE+int64(keySize)+8])

				if summaryKey == key {
					return int64(offset)
				}
				//lastSmallerOffset = int32(offset)
				pos += int64(totalSize)
				readIndicator = -1

			} else if readIndicator == 2 {
				if pos+8 > int64(len(buffer)) {
					break
				}
				offset := binary.LittleEndian.Uint64(buffer[pos:])

				if summaryKey == key {
					return int64(offset)
				}
				//lastSmallerOffset = int32(offset)
				pos += 8
				readIndicator = -1

			}

		}
		currentBlock++
	}
	return errorOffset
}

func (sst *SSTable) SearchData(key string, offset int64) []byte {
	startBlock := offset / int64(sst.BlockSize) //racunanje bloka od kog krece pretraga na osnovu offseta
	currentBlock := startBlock
	readIndicator := -1   //koristi se za identifikovnje da li se cita keySize(0), key(1), offset(2), pocetno stanje(-1)
	var helpBuffer []byte //koristi se za bajtove zapisa koji se prelamaju u sledeci blok
	var value []byte
	var keySize uint64
	var valueSize uint64
	totalSize := 0
	var remainingSize int

	var helpKeySize uint64 = 0

	for currentBlock < int64(sst.DataSegment.SegmentSize) { //prolazak kroz blokove data segmenta
		buffer, err := sst.BlockManager.ReadBlock(sst.DataFilePath, uint64(currentBlock), 'd', 0)
		if err != nil {
			return nil
		}
		// racunamo pocetnu poziciju samo za prvi blok, za svaki sledeci pocinjemo od nulte pozicije
		pos := int64(0)
		if currentBlock == startBlock {
			pos = offset % int64(sst.BlockSize)
		}

		for pos < int64(len(buffer)) { //prolazak kroz jedan blok
			if readIndicator == 0 { //znaci da se u baferu nalazi ostatak crc
				// citamo crc
				remainingSize = data.CRC_SIZE - len(helpBuffer) //ostatak crc koji je u trenutnom bloku
				pos += int64(remainingSize)                     //sad sam na poziciji na kojoj krece keySize
			} else if readIndicator == 3 { //znaci da se u baferu nalazi ostatak keySize
				// citamo keySize
				remainingSize = data.KEY_SIZE - len(helpBuffer) //ostatak keySize koji je u trenutnom bloku
				//dodam preostali dio keySize i pomjerim se u bloku
				helpBuffer = append(helpBuffer, buffer[:remainingSize]...)
				pos += int64(remainingSize) //sad sam na poziciji na kojoj krece valueSize
				//a u pomocnom baferu je cijeli keySize
				helpKeySize = binary.LittleEndian.Uint64(helpBuffer)
				helpBuffer = nil
			} else if readIndicator == 2 { // u pomocnom baferu je ostatak value
				remainingSize = int(valueSize) - len(helpBuffer) //velicina ostatka value
				if remainingSize > int(sst.BlockSize) {          //value se prelama kroz vise blokova
					helpBuffer = append(helpBuffer, buffer[:sst.BlockSize]...)
					break
				} else {
					helpBuffer = append(helpBuffer, buffer[:remainingSize]...)
					value = helpBuffer
					return value
				}

				//helpBuffer = nil
				//readIndicator = -1 //prelazimo na sledeci zapis
			} else if readIndicator == 1 {
				remainingSize := data.VALUE_SIZE - len(helpBuffer) //velicina ostatka valueSize
				//dodam preostali dio valueSize i pomjerim se u bloku
				helpBuffer = append(helpBuffer, buffer[:remainingSize]...)
				pos += int64(remainingSize)

				valueSize = binary.LittleEndian.Uint64(helpBuffer)
				helpBuffer = nil
				readIndicator = 2 //oznaka za citanje value
				//continue

			}

			//citanje novog zapisa
			if pos+data.CRC_SIZE > int64(len(buffer)) && readIndicator == -1 { //ne mogu da procitam crc
				helpBuffer = append(helpBuffer, buffer[pos:pos+data.CRC_SIZE]...)
				readIndicator = 0 //indikator da je u pomocnom baferu crc
				break
			}
			if pos+data.CRC_SIZE+data.KEY_SIZE > int64(len(buffer)) && readIndicator == -1 { //ne mogu da procitam key size u slucaju kad je procitan crc
				helpBuffer = append(helpBuffer, buffer[pos+data.CRC_SIZE:]...)
				readIndicator = 3 //indikator da je u pomocnom baferu keySize
				break
			}

			if readIndicator != 2 {
				if helpKeySize != 0 { //ako je keySize vec procitan
					keySize = helpKeySize
				} else {
					if readIndicator == 0 { //ako je crc prekoracio u drugi blok
						keySize = binary.LittleEndian.Uint64(buffer[pos : pos+data.CRC_SIZE])
					} else {
						keySize = binary.LittleEndian.Uint64(buffer[pos+data.CRC_SIZE : pos+data.CRC_SIZE+data.KEY_SIZE])
					}

				}
				if keySize == 0 { //VIDI JE LI NEOPHODNO
					break
				}

				if readIndicator != 3 && pos+data.CRC_SIZE+data.KEY_SIZE+data.VALUE_SIZE > int64(len(buffer)) { //ako je procitan key size a value size se prelama
					helpBuffer = append(helpBuffer, buffer[pos+data.CRC_SIZE+data.KEY_SIZE:]...)
					readIndicator = 1
					break
				}
				// if readIndicator == 3 && pos+4 > int32(len(buffer)){ //ako se prelomio key size a value size ne moze da se procita
				//nema logike da se desi ovaj slucaj
				// }
				//ako valueSize moze da se procita iz tog bloka
				//summaryKey = string(buffer[pos+4 : pos+4+int32(keySize)])
				if readIndicator == 3 {
					valueSize = binary.LittleEndian.Uint64(buffer[pos : pos+data.VALUE_SIZE])
				} else {
					valueSize = binary.LittleEndian.Uint64(buffer[pos+data.CRC_SIZE+data.KEY_SIZE : pos+data.CRC_SIZE+data.KEY_SIZE+data.VALUE_SIZE])
				}
				if readIndicator == 3 {
					if int64(uint64(pos)+data.VALUE_SIZE+keySize+valueSize) > int64(len(buffer)) { //ako ne moze da se procita citav value
						helpBuffer = append(helpBuffer, buffer[pos+data.VALUE_SIZE+int64(keySize):]...) //ostatak value
						readIndicator = 2
						break
					} else {
						//ako value moze da se procita iz tog bloka
						value = buffer[pos+data.VALUE_SIZE+int64(keySize) : pos+data.VALUE_SIZE+int64(keySize)+int64(valueSize)]
					}

				} else {
					if pos+data.CRC_SIZE+data.KEY_SIZE+data.VALUE_SIZE+int64(keySize)+int64(valueSize) > int64(len(buffer)) { //ako ne moze da se procita citav value
						helpBuffer = append(helpBuffer, buffer[pos+data.KEY_SIZE+data.CRC_SIZE+data.VALUE_SIZE+int64(keySize):]...) //ostatak value
						readIndicator = 2
						break
					} else {
						value = buffer[pos+data.CRC_SIZE+data.KEY_SIZE+data.VALUE_SIZE+int64(keySize) : pos+data.CRC_SIZE+data.KEY_SIZE+data.VALUE_SIZE+int64(keySize)+int64(valueSize)]
					}

				}
				if readIndicator == 3 { //za testiranje dodatno ovaj dio koda od 643-650 potencijalno nepotreban
					totalSize = 8 + int(keySize) + int(valueSize) + 1 + 8
				} else {
					totalSize = 24 + int(keySize) + int(valueSize) + 1 + 8

				}
				pos += int64(totalSize)
				readIndicator = -1
				return value

			} else if readIndicator == 2 {
				value = buffer[pos+data.CRC_SIZE+data.KEY_SIZE+data.VALUE_SIZE+int64(keySize) : pos+data.CRC_SIZE+data.KEY_SIZE+data.VALUE_SIZE+int64(keySize)+int64(valueSize)]
				return value
				//lastSmallerOffset = int32(offset)
				//pos += 4
				//readIndicator = -1

			}

		}
		currentBlock++
	}
	return nil
}
func (sst *SSTable) Get(key string) []byte { //pretraga se vrsi po kljucu, vraca se vrijednost pod tim kljucem
	//if zapis u bloom filteru nastavi trazenje
	//else vrati prazan zapis kao znak da je pretraga bezuspjesna

	//pronalazak pozicije u index dijelu na koju trebamo otici iz summary
	summaryOffset := sst.SearchSummary(key)
	if summaryOffset == -1 {
		return nil //nema kljuca u summary-ju
	}
	//pronalazak pozicije kljuca u data segmentu iz indeksa
	indexOffset := sst.SearchIndex(key, summaryOffset)
	if indexOffset == -1 {
		return nil //nema kljuca u indeksu
	}
	fmt.Println(indexOffset)
	//pronalazak vrijednosti i vracanje korisniku
	value := sst.SearchData(key, indexOffset)

	return value

}
