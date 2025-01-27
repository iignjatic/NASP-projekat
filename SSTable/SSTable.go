package SSTable

import (
	"NASP-PROJEKAT/BlockManager"
	"NASP-PROJEKAT/data"
	"encoding/binary"
	"fmt"
)

type SSTable struct {
	DataSegment     *DataSegment
	Index           *Index
	Summary         *Summary
	BlockManager    *BlockManager.BlockManager
	DataFilePath    string
	IndexFilePath   string
	SummaryFilePath string
}

// vraca velicinu niza rekorda data segmenta u bajtovima
func (sst *SSTable) getDataSize(records []*data.Record) uint32 {
	var totalSize uint32 = 0
	for i := 0; i < len(records); i++ {
		recordSize := sst.DataSegment.GetRecordSize(records[i])
		totalSize += recordSize
	}
	return uint32(totalSize)
}

// vraca velicinu niza rekorda indeksa  u bajtovima
func (sst *SSTable) getIndexSize(records []*data.Record) uint32 {
	var totalSize uint32 = 0
	for i := 0; i < len(records); i++ {
		recordSize := sst.Index.getRecordSize(records[i])
		totalSize += recordSize
	}
	return uint32(totalSize)
}

// vraca velicinu niza rekorda samarija  u bajtovima
func (sst *SSTable) getSummarySize(records []*data.Record) uint32 {
	var totalSize uint32 = 0
	var summaryCount = 0
	for i := 0; i < len(records); i++ { //ako je npr  10 (sample) zapisa proslo tek tad azuriramo brojac
		if summaryCount == int(sst.Summary.Sample) {
			recordSize := sst.Index.getRecordSize(records[i])
			totalSize += recordSize
			summaryCount = 0
		}
		summaryCount++
	}
	return uint32(totalSize)
}

func (sstable *SSTable) MakeSSTable(records []*data.Record) {
	//blokovi za data segment
	DataSize := sstable.getDataSize(records)                                    //ukupna velicina data dijela
	sstable.DataSegment.Blocks = make([]*data.Block, 2*DataSize/data.BlockSize) //bilo DataSize/BlockSize*2
	for i := range sstable.DataSegment.Blocks {
		sstable.DataSegment.Blocks[i] = &data.Block{} // Inicijalizacija svakog bloka
	}

	//blokovi za indeks
	IndexSize := sstable.getIndexSize(records)                             //ukupna velicina indeksa
	sstable.Index.Blocks = make([]*data.Block, 2*IndexSize/data.BlockSize) //bilo IndexSize/BlockSize*2
	for i := range sstable.Index.Blocks {
		sstable.Index.Blocks[i] = &data.Block{} // Inicijalizacija svakog bloka
	}

	//blokovi za summary
	sstable.Summary.Sample = 2 //OVDJE TREBA SAMPLE !!!!!!

	SummarySize := sstable.getSummarySize(records)                             //ukupna velicina samarija
	sstable.Summary.Blocks = make([]*data.Block, 3*SummarySize/data.BlockSize) //bilo SummarySize/BlockSize*2
	for i := range sstable.Summary.Blocks {
		sstable.Summary.Blocks[i] = &data.Block{} // Inicijalizacija svakog bloka
	}

	sstable.DataSegment.SegmentSize = uint32(len(sstable.DataSegment.Blocks))
	sstable.Index.SegmentSize = uint32(len(sstable.Index.Blocks))
	sstable.Summary.SegmentSize = uint32(len(sstable.Summary.Blocks))

	if len(records) > 0 {
		sstable.Summary.First = records[0].Key
		sstable.Summary.Last = records[len(records)-1].Key
	}

	sstable.MakeBlocks('d', records)
	sstable.MakeBlocks('i', records)
	sstable.MakeBlocks('s', records)

}

func (sst *SSTable) WriteSSTable() {
	var i uint32 = 0

	for i = 0; i < sst.DataSegment.SegmentSize; i++ {
		sst.BlockManager.WriteBlock(sst.DataSegment.Blocks[i], sst.DataFilePath, i, data.BlockSize)
	}
	for i = 0; i < sst.Index.SegmentSize; i++ {
		sst.BlockManager.WriteBlock(sst.Index.Blocks[i], sst.IndexFilePath, i, data.BlockSize)
	}
	for i = 0; i < sst.Summary.SegmentSize; i++ {
		sst.BlockManager.WriteBlock(sst.Summary.Blocks[i], sst.SummaryFilePath, i, data.BlockSize)
	}

}

// t je tip blokova, za indeks, data ili summary
func (sst *SSTable) MakeBlocks(t byte, records []*data.Record) {
	i := 0 //rekord
	var pos uint32
	var indicator byte = 'a'
	var offsetIndex uint32 = 0
	var offsetSummary uint32 = 0
	var summaryCount int32 = -1
	tempBlockSize := data.BlockSize
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
			if recordSize < uint32(tempBlockSize) && indicator != 'm' && indicator != 'l' { //ako moze cijeli stati odmah
				indicator = 'a' //all kao citav rekord je stao
				blocks[indexOfBlock].Records = append(blocks[indexOfBlock].Records, recordBytes...)
				tempBlockSize -= uint32(recordSize) //smanjimo velicinu bloka za velicinu unijetog rekorda
				i += 1                              //prelazak na sledeci rekord
				summaryIndicator = 0
				indexIndicator = 0

			} else if recordSize > uint32(tempBlockSize) && indicator != 'm' { //gigant je
				//if recordSize > BlockSize && tempBlockSize == BlockSize { //ako zapis ne moze stati u prazan blok
				summaryIndicator = 1 //znak da je u sledecem bloku dio prethodnog zapisa i
				// da se summaryCount ne treba povecati
				indexIndicator = 1
				//tempBlockSize = BlockSize
				indicator = 'f' //first
				//recordBytes := recordToBytes(records[i], recordSize, indicator)
				blocks[indexOfBlock].Records = append(blocks[indexOfBlock].Records, recordBytes[0:tempBlockSize]...)
				//recordSize -= uint32(tempBlockSize)
				pos = uint32(tempBlockSize)
				tempBlockSize = data.BlockSize //jer je blok popunjen do kraja
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
				recordSize = recordSize - pos
				if recordSize < uint32(data.BlockSize) { //ako ostatak zapisa moze stati u taj blok to je poslednji dio zapisa l''
					indicator = 'l'
					//recordBytes := recordToBytes(records[i], recordSize, indicator)
					blocks[indexOfBlock].Records = append(blocks[indexOfBlock].Records, recordBytes[pos:]...)
					i += 1
					summaryIndicator = 0
					indexIndicator = 0
					indicator = 'a' //resetovanje indikatora
					tempBlockSize -= recordSize
					if tempBlockSize == 0 {
						tempBlockSize = data.BlockSize
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
					blocks[indexOfBlock].Records = append(blocks[indexOfBlock].Records, recordBytes[pos:data.BlockSize+pos]...)
					//bilo ...append(blocks[indexOfBlock].records, recordBytes[pos:BlockSize]...)
					pos = pos + data.BlockSize
					tempBlockSize = data.BlockSize
					break //prelazimo na sledeci blok
				}

			} else if recordSize == data.BlockSize && indicator != 'm' { //OVO SE NIKAD NE DESAVA
				indicator = 'a'
				if t == 'd' {
					recordBytes = sst.DataSegment.RecordToBytes(records[i], recordSize, indicator)
				} else if t == 'i' {
					recordBytes = sst.Index.recordToBytes(records[i], recordSize, offsetIndex)
				} else {
					recordBytes = sst.Index.recordToBytes(records[i], recordSize, offsetSummary)
				}
				blocks[indexOfBlock].Records = append(blocks[indexOfBlock].Records, recordBytes...)
				tempBlockSize = data.BlockSize
				i += 1

			} else {
				break
			}
		}
	}
}

func (sst *SSTable) SearchSummary(key string) int32 {
	// if key < sst.Summary.First || key > sst.Summary.Last {
	// 	return -1
	// }
	var currentBlock uint32 = 0
	var lastSmallerOffset int32 = -1
	readIndicator := -1   //koristi se za identifikovnje da li se cita keySize(0), key(1), offset(2), pocetno stanje(-1)
	var helpBuffer []byte //koristi se za bajtove zapisa koji se prelamaju u sledeci blok
	var keySize uint32
	totalSize := 0
	var summaryKey string
	var helpKeySize uint32 = 0

	for currentBlock < sst.Summary.SegmentSize { //prolazak kroz blokove
		buffer, err := sst.BlockManager.ReadBlock(sst.SummaryFilePath, currentBlock)
		if err != nil {
			return -1
		}

		pos := 0

		for pos < len(buffer) { //prolazak kroz jedan blok
			// obrada nepotpunog zapisa iz prethodnog bloka
			if readIndicator == 0 { //znaci da se u baferu nalazi ostatak keySize
				// citamo keySize
				remainingSize := 4 - len(helpBuffer) //ostatak keySize koji je u trenutnom bloku
				//dodam preostali dio keySize i pomjerim se u bloku
				helpBuffer = append(helpBuffer, buffer[:remainingSize]...)
				pos += remainingSize //sad sam na poziciji na kojoj krece key
				//a u pomocnom baferu je cijeli keySize
				helpKeySize = binary.LittleEndian.Uint32(helpBuffer)
				helpBuffer = nil

			} else if readIndicator == 2 { // u pomocnom baferu je ostatak offseta
				remainingSize := 4 - len(helpBuffer) //velicina ostatka offseta
				helpBuffer = append(helpBuffer, buffer[:remainingSize]...)
				pos += remainingSize
				offset := binary.LittleEndian.Uint32(helpBuffer)
				if summaryKey > key {
					return lastSmallerOffset
				} else if summaryKey == key {
					return int32(offset)
				}
				lastSmallerOffset = int32(offset)
				helpBuffer = nil
				readIndicator = -1 //prelazimo na sledeci zapis
			} else if readIndicator == 1 {
				remainingSize := int(keySize) - len(helpBuffer) //velicina ostatka kljuca
				//dodam preostali dio key i pomjerim se u bloku
				helpBuffer = append(helpBuffer, buffer[:remainingSize]...)
				pos += remainingSize

				summaryKey = string(helpBuffer)
				helpBuffer = nil
				readIndicator = 2 //oznaka za citanje offseta

			}

			// citanje novog zapisa
			if pos+4 > len(buffer) && readIndicator == -1 {
				// nemamo dovoljno za keySize
				helpBuffer = append(helpBuffer, buffer[pos:]...)
				readIndicator = 0 //indikator da je u pomocnom baferu keySize
				break
			}
			if readIndicator != 2 {
				if helpKeySize != 0 { //ako je keySize vec procitan
					keySize = helpKeySize
				} else {
					keySize = binary.LittleEndian.Uint32(buffer[pos : pos+4])
				}
				if keySize == 0 { //VIDI JE LI NEOPHODNO
					//prelazim u sledeci blok jer je 0 znak da je do kraja tog bloka padding
					break
				}

				totalSize = 4 + int(keySize) + 4
				if pos+4+int(keySize) > len(buffer) { //ako ne moze da se procita citav key
					helpBuffer = append(helpBuffer, buffer[pos+4:]...)
					readIndicator = 1
					break
				}
				//ako key moze da se procita iz tog bloka
				summaryKey = string(buffer[pos+4 : pos+4+int(keySize)])
				if pos+4+int(keySize)+4 > len(buffer) { //ako ne moze da se procita citav offset
					helpBuffer = append(helpBuffer, buffer[pos+4+int(keySize):]...)
					readIndicator = 2
					break
				}
				//ako offset moze da se procita iz tog bloka
				offset := binary.LittleEndian.Uint32(buffer[pos+4+int(keySize) : pos+4+int(keySize)+4])

				if summaryKey > key {
					return lastSmallerOffset
				} else if summaryKey == key {
					return int32(offset)
				}
				lastSmallerOffset = int32(offset)
				pos += totalSize
				readIndicator = -1

			} else if readIndicator == 2 {
				if pos+4 > len(buffer) {
					break
				}
				offset := binary.LittleEndian.Uint32(buffer[pos : pos+4])

				if summaryKey > key {
					return lastSmallerOffset
				} else if summaryKey == key {
					return int32(offset)
				}
				lastSmallerOffset = int32(offset)
				pos += 4
				readIndicator = -1

			}

		}
		currentBlock++
	}
	return lastSmallerOffset
}

func (sst *SSTable) SearchIndex(key string, offset int32) int32 {
	startBlock := offset / int32(data.BlockSize) //racunanje bloka od kog krece pretraga na osnovu offseta
	// if offset%int32(BlockSize) != 0 {
	// 	startBlock++
	// }

	currentBlock := startBlock
	var errorOffset int32 = -1 //povratna vrijednost funkcije u slucaju da se kljuc ne nalazi u indeksu
	readIndicator := -1        //koristi se za identifikovnje da li se cita keySize(0), key(1), offset(2), pocetno stanje(-1)
	var helpBuffer []byte      //koristi se za bajtove zapisa koji se prelamaju u sledeci blok
	var keySize uint32
	totalSize := 0
	var summaryKey string
	var helpKeySize uint32 = 0

	for currentBlock < int32(sst.Index.SegmentSize) { //prolazak kroz blokove indexa
		buffer, err := sst.BlockManager.ReadBlock(sst.IndexFilePath, uint32(currentBlock))
		if err != nil {
			return -1
		}
		// racunamo pocetnu poziciju samo za prvi blok, za svaki sledeci pocinjemo od nulte pozicije
		pos := int32(0)
		if currentBlock == startBlock {
			pos = offset % int32(data.BlockSize)
		}

		for pos < int32(len(buffer)) { //prolazak kroz jedan blok
			// obrada nepotpunog zapisa iz prethodnog bloka
			if readIndicator == 0 { //znaci da se u baferu nalazi ostatak keySize
				// citamo keySize
				remainingSize := 4 - len(helpBuffer) //ostatak keySize koji je u trenutnom bloku
				//dodam preostali dio keySize i pomjerim se u bloku
				helpBuffer = append(helpBuffer, buffer[:remainingSize]...)
				pos += int32(remainingSize) //sad sam na poziciji na kojoj krece key
				//a u pomocnom baferu je cijeli keySize
				helpKeySize = binary.LittleEndian.Uint32(helpBuffer)
				helpBuffer = nil
				//readIndicator = 1 //prelazim na citanje key i offset
				//continue
			} else if readIndicator == 2 { // u pomocnom baferu je ostatak offseta
				remainingSize := 4 - len(helpBuffer) //velicina ostatka offseta
				helpBuffer = append(helpBuffer, buffer[:remainingSize]...)
				pos += int32(remainingSize)
				offset := binary.LittleEndian.Uint32(helpBuffer)
				if summaryKey == key {
					return int32(offset)
				}
				//lastSmallerOffset = int32(offset)
				helpBuffer = nil
				readIndicator = -1 //prelazimo na sledeci zapis
			} else if readIndicator == 1 {
				remainingSize := int(keySize) - len(helpBuffer) //velicina ostatka kljuca
				//dodam preostali dio key i pomjerim se u bloku
				helpBuffer = append(helpBuffer, buffer[:remainingSize]...)
				pos += int32(remainingSize)

				summaryKey = string(helpBuffer)
				helpBuffer = nil
				readIndicator = 2 //oznaka za citanje offseta
				//continue

			}

			// citanje novog zapisa
			if pos+4 > int32(len(buffer)) && readIndicator == -1 {
				// nemamo dovoljno za keySize
				helpBuffer = append(helpBuffer, buffer[pos:]...)
				readIndicator = 0 //indikator da je u pomocnom baferu keySize
				break
			}
			if readIndicator != 2 {
				if helpKeySize != 0 { //ako je keySize vec procitan
					keySize = helpKeySize
				} else {
					keySize = binary.LittleEndian.Uint32(buffer[pos : pos+4])
				}
				if keySize == 0 { //VIDI JE LI NEOPHODNO
					break
				}

				totalSize = 4 + int(keySize) + 4
				if pos+4+int32(keySize) > int32(len(buffer)) { //ako ne moze da se procita citav key
					helpBuffer = append(helpBuffer, buffer[pos+4:]...)
					readIndicator = 1
					break
				}
				//ako key moze da se procita iz tog bloka
				summaryKey = string(buffer[pos+4 : pos+4+int32(keySize)])
				if pos+4+int32(keySize)+4 > int32(len(buffer)) { //ako ne moze da se procita citav offset
					helpBuffer = append(helpBuffer, buffer[pos+4+int32(keySize):]...)
					readIndicator = 2
					break
				}
				//ako offset moze da se procita iz tog bloka
				offset := binary.LittleEndian.Uint32(buffer[pos+4+int32(keySize) : pos+4+int32(keySize)+4])

				if summaryKey == key {
					return int32(offset)
				}
				//lastSmallerOffset = int32(offset)
				pos += int32(totalSize)
				readIndicator = -1

			} else if readIndicator == 2 {
				if pos+4 > int32(len(buffer)) {
					break
				}
				offset := binary.LittleEndian.Uint32(buffer[pos:])

				if summaryKey == key {
					return int32(offset)
				}
				//lastSmallerOffset = int32(offset)
				pos += 4
				readIndicator = -1

			}

		}
		currentBlock++
	}
	return errorOffset
}

func (sst *SSTable) SearchData(key string, offset int32) []byte {
	startBlock := offset / int32(data.BlockSize) //racunanje bloka od kog krece pretraga na osnovu offseta
	currentBlock := startBlock
	readIndicator := -1   //koristi se za identifikovnje da li se cita keySize(0), key(1), offset(2), pocetno stanje(-1)
	var helpBuffer []byte //koristi se za bajtove zapisa koji se prelamaju u sledeci blok
	var value []byte
	var keySize uint32
	var valueSize uint32
	totalSize := 0
	var remainingSize int

	var helpKeySize uint32 = 0

	for currentBlock < int32(sst.DataSegment.SegmentSize) { //prolazak kroz blokove data segmenta
		buffer, err := sst.BlockManager.ReadBlock(sst.DataFilePath, uint32(currentBlock))
		if err != nil {
			return nil
		}
		// racunamo pocetnu poziciju samo za prvi blok, za svaki sledeci pocinjemo od nulte pozicije
		pos := int32(0)
		if currentBlock == startBlock {
			pos = offset % int32(data.BlockSize)
		}

		for pos < int32(len(buffer)) { //prolazak kroz jedan blok
			if readIndicator == 0 { //znaci da se u baferu nalazi ostatak crc
				// citamo crc
				remainingSize = 4 - len(helpBuffer) //ostatak crc koji je u trenutnom bloku
				pos += int32(remainingSize)         //sad sam na poziciji na kojoj krece keySize
			} else if readIndicator == 3 { //znaci da se u baferu nalazi ostatak keySize
				// citamo keySize
				remainingSize = 4 - len(helpBuffer) //ostatak keySize koji je u trenutnom bloku
				//dodam preostali dio keySize i pomjerim se u bloku
				helpBuffer = append(helpBuffer, buffer[:remainingSize]...)
				pos += int32(remainingSize) //sad sam na poziciji na kojoj krece valueSize
				//a u pomocnom baferu je cijeli keySize
				helpKeySize = binary.LittleEndian.Uint32(helpBuffer)
				helpBuffer = nil
			} else if readIndicator == 2 { // u pomocnom baferu je ostatak value
				remainingSize = int(valueSize) - len(helpBuffer) //velicina ostatka value
				if remainingSize > 70 {                          //value se prelama kroz vise blokova
					helpBuffer = append(helpBuffer, buffer[:data.BlockSize]...)
					break
				} else {
					helpBuffer = append(helpBuffer, buffer[:remainingSize]...)
					value = helpBuffer
					return value
				}

				//helpBuffer = nil
				//readIndicator = -1 //prelazimo na sledeci zapis
			} else if readIndicator == 1 {
				remainingSize := 4 - len(helpBuffer) //velicina ostatka valueSize
				//dodam preostali dio valueSize i pomjerim se u bloku
				helpBuffer = append(helpBuffer, buffer[:remainingSize]...)
				pos += int32(remainingSize)

				valueSize = binary.LittleEndian.Uint32(helpBuffer)
				helpBuffer = nil
				readIndicator = 2 //oznaka za citanje value
				//continue

			}

			//citanje novog zapisa
			if pos+4 > int32(len(buffer)) && readIndicator == -1 { //ne mogu da procitam crc
				helpBuffer = append(helpBuffer, buffer[pos:pos+4]...)
				readIndicator = 0 //indikator da je u pomocnom baferu crc
				break
			}
			if pos+8 > int32(len(buffer)) && readIndicator == -1 { //ne mogu da procitam key size u slucaju kad je procitan crc
				helpBuffer = append(helpBuffer, buffer[pos+4:]...)
				readIndicator = 3 //indikator da je u pomocnom baferu keySize
				break
			}

			if readIndicator != 2 {
				if helpKeySize != 0 { //ako je keySize vec procitan
					keySize = helpKeySize
				} else {
					if readIndicator == 0 { //ako je crc prekoracio u drugi blok
						keySize = binary.LittleEndian.Uint32(buffer[pos : pos+4])
					} else {
						keySize = binary.LittleEndian.Uint32(buffer[pos+4 : pos+8])
					}

				}
				if keySize == 0 { //VIDI JE LI NEOPHODNO
					break
				}

				if readIndicator != 3 && pos+8+4 > int32(len(buffer)) { //ako je procitan key size a value size se prelama
					helpBuffer = append(helpBuffer, buffer[pos+8:]...)
					readIndicator = 1
					break
				}
				// if readIndicator == 3 && pos+4 > int32(len(buffer)){ //ako se prelomio key size a value size ne moze da se procita
				//nema logike da se desi ovaj slucaj
				// }
				//ako valueSize moze da se procita iz tog bloka
				//summaryKey = string(buffer[pos+4 : pos+4+int32(keySize)])
				if readIndicator == 3 {
					valueSize = binary.LittleEndian.Uint32(buffer[pos : pos+4])
				} else {
					valueSize = binary.LittleEndian.Uint32(buffer[pos+8 : pos+12])
				}
				if readIndicator == 3 {
					if pos+4+int32(keySize)+int32(valueSize) > int32(len(buffer)) { //ako ne moze da se procita citav value
						helpBuffer = append(helpBuffer, buffer[pos+4+int32(keySize):]...) //ostatak value
						readIndicator = 2
						break
					} else {
						//ako value moze da se procita iz tog bloka
						value = buffer[pos+4+int32(keySize) : pos+4+int32(keySize)+int32(valueSize)]
					}

				} else {
					if pos+12+int32(keySize)+int32(valueSize) > int32(len(buffer)) { //ako ne moze da se procita citav value
						helpBuffer = append(helpBuffer, buffer[pos+12+int32(keySize):]...) //ostatak value
						readIndicator = 2
						break
					} else {
						value = buffer[pos+12+int32(keySize) : pos+12+int32(keySize)+int32(valueSize)]
					}

				}
				if readIndicator == 3 { //za testiranje dodatno ovaj dio koda od 643-650 potencijalno nepotreban
					totalSize = 4 + int(keySize) + int(valueSize) + 1 + 10
				} else {
					totalSize = 12 + int(keySize) + int(valueSize) + 1 + 10

				}
				pos += int32(totalSize)
				readIndicator = -1
				return value

			} else if readIndicator == 2 {
				value = buffer[pos+12+int32(keySize) : pos+12+int32(keySize)+int32(valueSize)]
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
