package main

import (
	"NASP-PROJEKAT/BlockCache"
	"NASP-PROJEKAT/BlockManager"
	"NASP-PROJEKAT/SSTable"
	"NASP-PROJEKAT/data"
	"NASP-PROJEKAT/lruCache"
	"NASP-PROJEKAT/memtable"
	merkleStablo "NASP-PROJEKAT/merkle-stablo"
	"NASP-PROJEKAT/tokenBucket"
	"NASP-PROJEKAT/wal"
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	BlockSize        uint64 `json:"BlockSize"`
	BlocksPerSegment uint64 `json:"BlocksPerSegment"`
	MemTableSize     uint64 `json:"MemTableSize"`
	MemTableCount    uint64 `json:"MemTableCount"`
	MemTableType     string `json:"MemTableType"`
	CacheSize        uint64 `json:"CacheSize"`
	SummarySample    uint64 `json:"SummarySample"`
	MaxTokens        int64  `json:"MaxTokens"`
	ResetInterval    int64  `json:"ResetInterval"`
}
type KeyValuePair struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func LoadKeyValuePairsFromJSON(filePath string) ([]KeyValuePair, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var pairs []KeyValuePair
	err = json.Unmarshal(data, &pairs)
	if err != nil {
		return nil, err
	}

	return pairs, nil
}


func LoadConfig(path string) (Config, error) {
	var config Config

	file, err := os.Open(path)
	if err != nil {
		return config, fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	bytes, err := io.ReadAll(file)
	if err != nil {
		return config, fmt.Errorf("failed to read config file: %w", err)
	}

	err = json.Unmarshal(bytes, &config)
	if err != nil {
		return config, fmt.Errorf("failed to parse config file: %w", err)
	}

	return config, nil
}

func main() {
	// default vrijednosti konfiguracije
	var BLOCK_SIZE uint64 = 70
	var BLOCKS_PER_SEGMENT uint64 = 4
	var MEMTABLE_SIZE uint64 = 3
	var MEMTABLE_COUNT uint64 = 2
	var MEMTABLE_TYPE string = "hashmap"
	var CACHE_SIZE uint32 = 10
	var SUMMARY_SAMPLE uint64 = 5
	var MAX_TOKENS int64 = 10
	var RESET_INTERVAL int64 = 30
	var showBlocks bool = false
	
	config, err := LoadConfig("../config.json")
	if err != nil {
		log.Fatalf("Failed to unmarshal JSON: %v", err)
	} else {
		BLOCK_SIZE = uint64(config.BlockSize)
		BLOCKS_PER_SEGMENT = uint64(config.BlocksPerSegment)
		MEMTABLE_SIZE = uint64(config.MemTableSize)
		MEMTABLE_COUNT = uint64(config.MemTableCount)
		MEMTABLE_TYPE = config.MemTableType
		CACHE_SIZE = uint32(config.CacheSize)
		SUMMARY_SAMPLE = uint64(config.SummarySample)
		MAX_TOKENS = int64(config.MaxTokens)
		RESET_INTERVAL = int64(config.ResetInterval)
	}

	recordCache := lruCache.MakeLRUCache(int(CACHE_SIZE))

	memtable, err := memtable.CreateMemtableManager(MEMTABLE_TYPE, int(MEMTABLE_COUNT), int(MEMTABLE_SIZE))
	if err != nil {
		panic(err)
	}

	LRUlist := &BlockCache.LRUlist{}
	blockMap := make(map[string]*BlockCache.BlockNode)
	blockCache := &BlockCache.BlockCache{
		LRUlist:  LRUlist,
		Capacity: 10,
		BlockMap: blockMap,
	}

	blockManager := &BlockManager.BlockManager{
		BlockCache: *blockCache,
		BlockSize:  BLOCK_SIZE,
	}
	dataSeg := &SSTable.DataSegment{
		BlockManager: *blockManager,
	}
	index := &SSTable.Index{}
	summary := &SSTable.Summary{
		Sample: SUMMARY_SAMPLE,
	}

	files, _ := ioutil.ReadDir("../SSTable/files")
	numOfSSTables := len(files)

	//var input uint32
	//var key string
	var value []byte

	//var sstableName string
	const tokenBucketKey = "token_bucket"
	tb := &tokenBucket.TokenBucket{}

	w := wal.NewWal(blockManager, BLOCK_SIZE, BLOCKS_PER_SEGMENT)

	// OBRIŠI prethodne kompletno flushovane WAL segmente
	flushInfos, err := wal.LoadFlushInfoFromFile()
	if err != nil {
		log.Fatalf("Greska pri ucitavanju FlushInfo: %v", err)
	}
	for _, info := range flushInfos {
		w.DeleteFullyFlushedSegments(info)
	}

	// 1. Učitavanje neflushovanih WAL zapisa
	pendingRecords, err := w.ReadAllSegmentsCP(true)
	if err != nil {
		log.Fatalf("Greska pri čitanju WAL-a pri pokretanju: %v", err)
	}

	// 2. Učitavanje u memtable (rekonstrukcija iz WAL-a)
	if len(pendingRecords) > 0 {
		recs, err := memtable.LoadFromWal(pendingRecords)
		if err != nil {
			log.Fatalf("Greska pri restorovanju MemTable iz WAL-a: %v", err)
		}

		// (opciono) za debug
		for _, r := range recs {
			fmt.Println(r)
		}
	}

	// PROVJERA DA LI TOKENBUCKET VEC POSTOJI U SISTEMU

	found := false

	if _, exists, _ := memtable.Get(tokenBucketKey); exists {
		//fmt.Println("Zapis je pronadjen : ", string(value))
		found = true
		//fmt.Println("TOKENBUCKET JE PRONADJEN U SISTEMU PRILIKOM POKRETANJA PROGRAMA U MEMTABELI")
	}

	// else if SEARCHCACHE != nil
	// 	continue

	if !found {
		if value = recordCache.Get(tokenBucketKey); value != nil {
			//fmt.Println("Zapis je pronadjen : ", value)
			//fmt.Println("TOKENBUCKET JE PRONADJEN U SISTEMU PRILIKOM POKRETANJA PROGRAMA U RECORDCACHE")
			found = true
		}
	}

	if !found {
		for i := numOfSSTables; i > 0; i-- { //prolazak kroz sve sstabele
			// Kreiranje SSTable-a
			sst := &SSTable.SSTable{
				DataSegment:     dataSeg,
				Index:           index,
				Summary:         summary,
				BlockManager:    blockManager,
				DataFilePath:    "../SSTable/files/sstable_" + strconv.Itoa(i) + "/data" + strconv.Itoa(i) + ".bin",
				IndexFilePath:   "../SSTable/files/sstable_" + strconv.Itoa(i) + "/index" + strconv.Itoa(i) + ".bin",
				SummaryFilePath: "../SSTable/files/sstable_" + strconv.Itoa(i) + "/summary" + strconv.Itoa(i) + ".bin",
				BlockSize:       BLOCK_SIZE,
			}
			summarySize, err := os.Stat(sst.SummaryFilePath)
			if err != nil {
				panic(err)
			}
			indexSize, err := os.Stat(sst.IndexFilePath)
			if err != nil {
				panic(err)
			}
			dataSize, err := os.Stat(sst.DataFilePath)
			if err != nil {
				panic(err)
			}
			sst.Summary.SegmentSize = uint64(summarySize.Size()) / BLOCK_SIZE
			sst.Index.SegmentSize = uint64(indexSize.Size()) / BLOCK_SIZE
			sst.DataSegment.SegmentSize = uint64(dataSize.Size()) / BLOCK_SIZE
			value = sst.Get(tokenBucketKey)
			if value == nil {
				continue
			} else if len(value) == 0 {
				fmt.Println("Zapis je obrisan")
				//	fmt.Println("TOKENBUCKET JE OBRISAN IZ SISTEMA VIDJENO PREKO SSTABLE PRILIKOM TRAZENJA KADA SE POKRENE APLIKACIJA")
				break
			} else {
				//fmt.Println("Zapis je pronadjen : ", string(value))
				recordCache.Put(tokenBucketKey, value)
				//fmt.Println("STA SAM PISAO U RECORDCACHE KAD SAM GA UPDATOVAO POSLE SSTABLE PRILIKOM GET ZA TOKENBUCKET PRILIKOM POKRETANJA PROGRAMA ", string(value))
				//update CACHE!!!!!!!!!!!!!!!!!!
				found = true
				//fmt.Println("TOKENBUCKET PRONADJEN U SISTEMU PRILIKOM TRAZENJA KADA SE POKRENE APLIKACIJA U SSTABLE")
				break
			}

		}
		if value == nil {
			//fmt.Println("Zapis nije pronadjen")
			//fmt.Println("TOKENBUCKET NIJE PRONADJEN U SISTEMU PRILIKOM TRAZENJA KADA SE POKRENE APLIKACIJA U SSTABLE")
		}
	}

	// AKO TOKENBUCKET NIJE PRONADJEN U SISTEMU, ONDA GA TREBA NAPRAVITI

	if !found {
		//fmt.Println("TOKENBUCKET NIJE PRONADJEN U SISTEMU PRILIKOM TRAZENJA KADA SE POKRENE APLIKACIJA ZNACI NIGDJE PRILLIKOM POKRETANJA SISTEMA NIJE PRONADJEN")
		tb = tokenBucket.NewTokenBucket(MAX_TOKENS, RESET_INTERVAL)
		tokenBucketState, err := tb.SerializeState()
		if err != nil {
			log.Fatalf("Greska prilikom serijalizacije tokenBucket-a: %v", err)
		}

		//fmt.Println("OVO JE TOKENBACKETSTATE ", tokenBucketState)

		// DODAJEM TOKENBUCKET U SISTEM

		// write to WAL
		tokenBucketState = append(tokenBucketState, 1)
		rec := data.NewRecord(tokenBucketKey, tokenBucketState)
		w.AddRecord(rec)

		// write to MemTable
		flushedRecords, flush, err := memtable.Put(wal.NoZerosRecord(data.DeepCopyRecord(rec)))
		if err != nil {
			panic(err)
		}

		// if len(flushedRecords) > 0 {
		// 	for i, record := range flushedRecords {
		// 		fmt.Printf("Element %d: %+v\n", i, record)
		// 	}
		// } else {
		// 	fmt.Printf("Prazan niz")
		// }

		recordCache.Put(tokenBucketKey, tokenBucketState) // ovdje sam dodao upis u recordCache
		//	fmt.Println("TOKENBUCKET JE DODAT U SISTEM PRILIKOM POKRETANJA APLIKACIJE I UPISAN U RECORDCACHE, U RECORDCACHE SAM DODAO TOKENBUCKET STATE ", tokenBucketState)

		if flush {
			// flushedRecords je niz pokazivaca za sstable
			numOfSSTables++
			newSSTable := "sstable_" + strconv.Itoa(numOfSSTables)
			sstPath := "../SSTable/files/" + newSSTable

			metadataPath := "../SSTable/files/" + newSSTable + "/metadata" + strconv.Itoa(numOfSSTables)

			err := os.MkdirAll(sstPath, 0755)
			if err != nil {
				fmt.Println("Error creating folder:", err)
				return
			}

			err2 := os.MkdirAll(metadataPath, 0755)
			if err2 != nil {
				fmt.Println("Error creating metadata folder:", err)
				return
			}

			sst := &SSTable.SSTable{
				DataSegment:     dataSeg,
				Index:           index,
				Summary:         summary,
				BlockManager:    blockManager,
				DataFilePath:    "../SSTable/files/" + newSSTable + "/data" + strconv.Itoa(numOfSSTables) + ".bin",
				IndexFilePath:   "../SSTable/files/" + newSSTable + "/index" + strconv.Itoa(numOfSSTables) + ".bin",
				SummaryFilePath: "../SSTable/files/" + newSSTable + "/summary" + strconv.Itoa(numOfSSTables) + ".bin",
				BlockSize:       BLOCK_SIZE,
			}

			sst.MakeSSTable(flushedRecords)
			sst.Index = index
			sst.Summary = summary
			sst.WriteSSTable()

			// //  ucitavanje blokova iz fajla
			dataBlocks := dataSeg.BlocksToMerkle("../SSTable/files/" + newSSTable + "/data" + strconv.Itoa(numOfSSTables) + ".bin")

			// // pravim listove stabla
			dataLeafNodes := merkleStablo.CreateLeafNodes(dataBlocks)

			// // 	//  pravim originalno Merkle stablo
			dataOriginalRoot := merkleStablo.BuildMerkleTreeBottomUp(dataLeafNodes)

			// // kreiram MerkleTree objekat
			//dataOriginalTree := &merkleStablo.MerkleTree{Root: dataOriginalRoot}
			//fmt.Printf("Korjen hash-a: %x\n", dataOriginalTree.Root.Hash)

			dataSerializeFileName := metadataPath + "/merkle_tree" + strconv.Itoa(numOfSSTables) + ".bin"

			// // otvaranje fajla za pisanje
			dataFile, err := os.Create(dataSerializeFileName)
			if err != nil {
				log.Fatalf("Greska pri kreiranju fajla: %v", err)
			}
			defer dataFile.Close() // Automatski zatvara fajl na kraju

			// // serijalizacija Merkle stabla
			err = merkleStablo.SerializeMerkleTree(dataOriginalRoot, dataFile)
			if err != nil {
				log.Fatalf("Greska pri serijalizaciji: %v", err)
			}

			flushInfo, err := w.CreateFlushInfo(flushedRecords)
			if err != nil {
				fmt.Println("Greska pri kreiranju FlushInfo:", err)
			} else {
				wal.SaveFlushInfoToFile(flushInfo)
				w.DeleteFullyFlushedSegments(flushInfo)
			}

		}
		w.ShowBlocks(showBlocks)
		//fmt.Println("TOKENBUCKET JE NAPRAVLJEN I DODAT U SISTEM PRILIKOM POKRETANJA APLIKACIJE")
	}

	for {

		fmt.Println(" * KEY - VALUE ENGINE * ")
		fmt.Println("Izaberite opciju: ")
		fmt.Println("1. GET [ key ] ")
		fmt.Println("2. PUT [ key, value] ")
		fmt.Println("3. DELETE [ key ]")
		fmt.Println("4. PROVJERA INTEGRITETA PODATAKA [ naziv  SSTable-a npr. sstable_1]")
		fmt.Println("5. AUTOMATIZOVAN UNOS PODATAKA")

		reader := bufio.NewReader(os.Stdin)
		num, _ := reader.ReadString('\n')
		num = strings.TrimSpace(num)

		input, err := strconv.Atoi(num)
		if err != nil {
			fmt.Println("Greška: nije broj.")
			return
		}
		// fmt.Scan(&input)
		// input = 3

		//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		// RADIM GET ZA TOKENBUCKET

		found := false
		var tokenBucketStateValue []byte

		if _, exists, _ := memtable.Get(tokenBucketKey); exists {
			//fmt.Println("Zapis je pronadjen : ", string(value))
			found = true
			tmp, _, _ := memtable.Get(tokenBucketKey)
			tokenBucketStateValue = tmp.Value
			//fmt.Println("ZAPIS JE PRONADJEN U SISTEMU PRILIKOM GET OPERACIJE U MEMTABELI  TRAZIO SAM TOKENBUCKET I NASAO ", tokenBucketStateValue)

		}

		// else if SEARCHCACHE != nil
		// 	continue

		if !found {
			if value = recordCache.Get(tokenBucketKey); value != nil {
				//fmt.Println("Zapis je pronadjen : ", string(value))
				found = true
				tokenBucketStateValue = value
				//fmt.Println("ZAPIS JE PRONADJEN U SISTEMU PRILIKOM GET OPERACIJE U RECORDCACHE  TRAZIO SAM TOKENBUCKET I NASAO ", tokenBucketStateValue)
			}
		}

		if !found {
			for i := numOfSSTables; i > 0; i-- { //prolazak kroz sve sstabele
				// Kreiranje SSTable-a
				sst := &SSTable.SSTable{
					DataSegment:     dataSeg,
					Index:           index,
					Summary:         summary,
					BlockManager:    blockManager,
					DataFilePath:    "../SSTable/files/sstable_" + strconv.Itoa(i) + "/data" + strconv.Itoa(i) + ".bin",
					IndexFilePath:   "../SSTable/files/sstable_" + strconv.Itoa(i) + "/index" + strconv.Itoa(i) + ".bin",
					SummaryFilePath: "../SSTable/files/sstable_" + strconv.Itoa(i) + "/summary" + strconv.Itoa(i) + ".bin",
					BlockSize:       BLOCK_SIZE,
				}
				summarySize, err := os.Stat(sst.SummaryFilePath)
				if err != nil {
					panic(err)
				}
				indexSize, err := os.Stat(sst.IndexFilePath)
				if err != nil {
					panic(err)
				}
				dataSize, err := os.Stat(sst.DataFilePath)
				if err != nil {
					panic(err)
				}
				sst.Summary.SegmentSize = uint64(summarySize.Size()) / BLOCK_SIZE
				sst.Index.SegmentSize = uint64(indexSize.Size()) / BLOCK_SIZE
				sst.DataSegment.SegmentSize = uint64(dataSize.Size()) / BLOCK_SIZE
				value = sst.Get(tokenBucketKey)
				tokenBucketStateValue = value
				if value == nil {
					continue
				} else if len(value) == 0 {
					//fmt.Println("Zapis je obrisan")
					break
				} else {
					//fmt.Println("Zapis je pronadjen : ", string(value))
					recordCache.Put(tokenBucketKey, value)
					//update CACHE!!!!!!!!!!!!!!!!!!
					found = true
					tokenBucketStateValue = value
					//fmt.Println("ZAPIS JE PRONADJEN U SISTEMU PRILIKOM GET OPERACIJE U RECORDCACHE  TRAZIO SAM TOKENBUCKET I NASAO ", tokenBucketStateValue)
					break
				}

			}
			if value == nil {
				//fmt.Println("Zapis nije pronadjen")
			}
		}

		if found {
			// MIJENJAM STANJE TOKENBUCKETA U SISTEMU
			err := tb.DeserializeState(tokenBucketStateValue)
			if err != nil {
				// Ako deserijalizacija nije uspjela, napravi novi TokenBucket
				fmt.Println("Greška pri deserijalizaciji TokenBucket-a")
				continue
			}
			tb.DecreaseResetTokens()
			updatedTokenBucketState, err := tb.SerializeState()
			if err != nil {
				fmt.Println("Greška pri serijalizaciji azuriranog TokenBucket-a:", err)
				continue
			}

			//fmt.Println("USPIO SAM DA DESERIJALIZUJEM TOKENBUCKET DA GA PROMJENIM I DESERJALIZUJEM")

			// PISEM AZURIRANI TOKENBUCKET U SISTEM

			// write to WAL
			updatedTokenBucketState = append(updatedTokenBucketState, 1)
			rec := data.NewRecord(tokenBucketKey, updatedTokenBucketState)
			w.AddRecord(rec)

			// write to MemTable
			flushedRecords, flush, err := memtable.Put(wal.NoZerosRecord(data.DeepCopyRecord(rec)))
			if err != nil {
				panic(err)
			}

			// if len(flushedRecords) > 0 {
			// 	for i, record := range flushedRecords {
			// 		fmt.Printf("Element %d: %+v\n", i, record)
			// 	}
			// } else {
			// 	fmt.Printf("Prazan niz")
			// }

			recordCache.Put(tokenBucketKey, updatedTokenBucketState) // ovdje sam dodao upis u recordCache
			//fmt.Println("TOKENBUCKET JE AZURIRAN U SISTEMU PRILIKOM GET OPERACIJE I UPISAN U RECORDCACHE, U RECORDCACHE SAM DODAO TOKENBUCKET STATE. ZNACI UPISUJE SE AZURIRANI TOKENBUCKET U SISTEM ", updatedTokenBucketState)

			if flush {
				// flushedRecords je niz pokazivaca za sstable
				numOfSSTables++
				newSSTable := "sstable_" + strconv.Itoa(numOfSSTables)
				sstPath := "../SSTable/files/" + newSSTable

				metadataPath := "../SSTable/files/" + newSSTable + "/metadata" + strconv.Itoa(numOfSSTables)

				//fmt.Println("OVO JE METADATAPATH")
				//fmt.Println(metadataPath)

				err := os.MkdirAll(sstPath, 0755)
				if err != nil {
					fmt.Println("Error creating folder:", err)
					return
				}

				err2 := os.MkdirAll(metadataPath, 0755)
				if err2 != nil {
					fmt.Println("Error creating metadata folder:", err)
					return
				}

				sst := &SSTable.SSTable{
					DataSegment:     dataSeg,
					Index:           index,
					Summary:         summary,
					BlockManager:    blockManager,
					DataFilePath:    "../SSTable/files/" + newSSTable + "/data" + strconv.Itoa(numOfSSTables) + ".bin",
					IndexFilePath:   "../SSTable/files/" + newSSTable + "/index" + strconv.Itoa(numOfSSTables) + ".bin",
					SummaryFilePath: "../SSTable/files/" + newSSTable + "/summary" + strconv.Itoa(numOfSSTables) + ".bin",
					BlockSize:       BLOCK_SIZE,
				}

				sst.MakeSSTable(flushedRecords)
				sst.Index = index
				sst.Summary = summary
				sst.WriteSSTable()

				//fmt.Println("OVO JE PUTANJA")
				//fmt.Println("../SSTable/files/" + newSSTable + "/data" + strconv.Itoa(numOfSSTables) + ".bin")
				// //  ucitavanje blokova iz fajla
				dataBlocks := dataSeg.BlocksToMerkle("../SSTable/files/" + newSSTable + "/data" + strconv.Itoa(numOfSSTables) + ".bin")

				// // pravim listove stabla
				dataLeafNodes := merkleStablo.CreateLeafNodes(dataBlocks)

				//fmt.Println("OVO SU DATALEAFNODES")
				//fmt.Println(dataLeafNodes)

				//fmt.Println("JEL OVO OK ZA MERKLE STABLO")

				// // 	//  pravim originalno Merkle stablo
				dataOriginalRoot := merkleStablo.BuildMerkleTreeBottomUp(dataLeafNodes)

				//fmt.Println("OVO JE DATAORIGINALROOT")
				//fmt.Println(dataOriginalRoot)

				// // kreiram MerkleTree objekat
				//dataOriginalTree := &merkleStablo.MerkleTree{Root: dataOriginalRoot}
				//fmt.Printf("Korjen hash-a: %x\n", dataOriginalTree.Root.Hash)

				dataSerializeFileName := metadataPath + "/merkle_tree" + strconv.Itoa(numOfSSTables) + ".bin"

				// // otvaranje fajla za pisanje
				dataFile, err := os.Create(dataSerializeFileName)
				if err != nil {
					log.Fatalf("Greska pri kreiranju fajla: %v", err)
				}
				defer dataFile.Close() // Automatski zatvara fajl na kraju

				// // serijalizacija Merkle stabla
				err = merkleStablo.SerializeMerkleTree(dataOriginalRoot, dataFile)
				if err != nil {
					log.Fatalf("Greska pri serijalizaciji: %v", err)
				}

				flushInfo, err := w.CreateFlushInfo(flushedRecords)
				if err != nil {
					fmt.Println("Greska pri kreiranju FlushInfo:", err)
				} else {
					wal.SaveFlushInfoToFile(flushInfo)
					w.DeleteFullyFlushedSegments(flushInfo)
				}

			}
			w.ShowBlocks(showBlocks)

			//fmt.Println("TOKENBUCKET JE AZURIRAN U SISTEMU POSLE GET OPERACIJE")

			if tb.GetCurrentNumberOfTokens() < 0 {
				fmt.Println("Potrosili ste sve tokene, operacija koju ste posljednje unijeli nije izvrsena, probajte ponovo kasnije!")

				continue
			}

		} else {
			panic("TokenBucket bi trebao da bude pronadjen u sistemu ali nije!!!")

		}
		//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

		if input == 1 {
			//GET operacija
			reader := bufio.NewReader(os.Stdin)
			fmt.Print("Unesite ključ: ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)
			//fmt.Scan(&key)
			//key = "key2"

			if key == tokenBucketKey {
				fmt.Println("Korisniku je zabranjena bilo kakva manipulacija sa tokenBucketom")
				continue
			}

			if record, exists, deleted := memtable.Get(key); exists {
				if !deleted {
					fmt.Println("Zapis je pronadjen: ", string(record.Value))
				} else {
					fmt.Println("Zapis je obrisan")
				}
				continue
			}

			// else if SEARCHCACHE != nil
			// 	continue

			if value = recordCache.Get(key); value != nil {
				fmt.Println("Zapis je pronadjen : ", string(value))
				//fmt.Println("ZAPIS JE PRONADJEN U SISTEMU PRILIKOM GET OPERACIJE U RECORDCACHE")
				continue
			}

			for i := numOfSSTables; i > 0; i-- { //prolazak kroz sve sstabele
				// Kreiranje SSTable-a
				sst := &SSTable.SSTable{
					DataSegment:     dataSeg,
					Index:           index,
					Summary:         summary,
					BlockManager:    blockManager,
					DataFilePath:    "../SSTable/files/sstable_" + strconv.Itoa(i) + "/data" + strconv.Itoa(i) + ".bin",
					IndexFilePath:   "../SSTable/files/sstable_" + strconv.Itoa(i) + "/index" + strconv.Itoa(i) + ".bin",
					SummaryFilePath: "../SSTable/files/sstable_" + strconv.Itoa(i) + "/summary" + strconv.Itoa(i) + ".bin",
					BlockSize:       BLOCK_SIZE,
				}
				summarySize, err := os.Stat(sst.SummaryFilePath)
				if err != nil {
					panic(err)
				}
				indexSize, err := os.Stat(sst.IndexFilePath)
				if err != nil {
					panic(err)
				}
				dataSize, err := os.Stat(sst.DataFilePath)
				if err != nil {
					panic(err)
				}
				sst.Summary.SegmentSize = uint64(summarySize.Size()) / BLOCK_SIZE
				sst.Index.SegmentSize = uint64(indexSize.Size()) / BLOCK_SIZE
				sst.DataSegment.SegmentSize = uint64(dataSize.Size()) / BLOCK_SIZE
				value = sst.Get(key)
				if value == nil {
					continue
				} else if len(value) == 0 {
					fmt.Println("Zapis je obrisan")
					break
				} else {
					fmt.Println("Zapis je pronadjen : ", string(value))
					recordCache.Put(key, value)
					//fmt.Println("ZAPIS JE DODAT U RECORDCACHE PRILIKOM GET OPERACIJE U SSTABLE")

					//update CACHE!!!!!!!!!!!!!!!!!!
					break
				}

			}

			if value == nil {
				fmt.Println("Zapis nije pronadjen")
			}

		} else if input == 2 {
			// put
			fmt.Print("Unesite ključ: ")
			reader := bufio.NewReader(os.Stdin)
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)
			fmt.Print("Unesite vrijednost: ")
			valueStr, _ := reader.ReadString('\n')
			value = []byte(strings.TrimSpace(valueStr))
			//fmt.Scan(&key, &value)

			if key == tokenBucketKey {
				fmt.Println("Korisniku je zabranjena bilo kakva manipulacija sa tokenBucketom")
				continue
			}

			// write to WAL
			rec := data.NewRecord(key, []byte(value))
			w.AddRecord(rec)

			// write to MemTable
			flushedRecords, flush, err := memtable.Put(wal.NoZerosRecord(data.DeepCopyRecord(rec)))
			if err != nil {
				panic(err)
			}

			if len(flushedRecords) > 0 {
				for i, record := range flushedRecords {
					fmt.Printf("Element %d: %+v\n", i, record)
				}
			} else {
				fmt.Printf("Prazan niz")
			}

			recordCache.Put(key, []byte(value)) // ovdje sam dodao upis u recordCache
			//fmt.Println("ZAPIS JE DODAT U SISTEM PRILIKOM PUT OPERACIJE I UPISAN U RECORDCACHE, U RECORDCACHE SAM DODAO KEY I VALUE ", key, string(value))

			if flush {
				// flushedRecords je niz pokazivaca za sstable
				numOfSSTables++
				newSSTable := "sstable_" + strconv.Itoa(numOfSSTables)
				sstPath := "../SSTable/files/" + newSSTable

				metadataPath := "../SSTable/files/" + newSSTable + "/metadata" + strconv.Itoa(numOfSSTables)

				err := os.MkdirAll(sstPath, 0755)
				if err != nil {
					fmt.Println("Error creating folder:", err)
					return
				}

				err2 := os.MkdirAll(metadataPath, 0755)
				if err2 != nil {
					fmt.Println("Error creating metadata folder:", err)
					return
				}

				sst := &SSTable.SSTable{
					DataSegment:     dataSeg,
					Index:           index,
					Summary:         summary,
					BlockManager:    blockManager,
					DataFilePath:    "../SSTable/files/" + newSSTable + "/data" + strconv.Itoa(numOfSSTables) + ".bin",
					IndexFilePath:   "../SSTable/files/" + newSSTable + "/index" + strconv.Itoa(numOfSSTables) + ".bin",
					SummaryFilePath: "../SSTable/files/" + newSSTable + "/summary" + strconv.Itoa(numOfSSTables) + ".bin",
					BlockSize:       BLOCK_SIZE,
				}

				sst.MakeSSTable(flushedRecords)
				sst.Index = index
				sst.Summary = summary
				sst.WriteSSTable()

				// //  ucitavanje blokova iz fajla
				dataBlocks := dataSeg.BlocksToMerkle("../SSTable/files/" + newSSTable + "/data" + strconv.Itoa(numOfSSTables) + ".bin")

				// // pravim listove stabla
				dataLeafNodes := merkleStablo.CreateLeafNodes(dataBlocks)

				// // 	//  pravim originalno Merkle stablo
				dataOriginalRoot := merkleStablo.BuildMerkleTreeBottomUp(dataLeafNodes)

				// // kreiram MerkleTree objekat
				//dataOriginalTree := &merkleStablo.MerkleTree{Root: dataOriginalRoot}
				//fmt.Printf("Korjen hash-a: %x\n", dataOriginalTree.Root.Hash)

				dataSerializeFileName := metadataPath + "/merkle_tree" + strconv.Itoa(numOfSSTables) + ".bin"

				// // otvaranje fajla za pisanje
				dataFile, err := os.Create(dataSerializeFileName)
				if err != nil {
					log.Fatalf("Greska pri kreiranju fajla: %v", err)
				}
				defer dataFile.Close() // Automatski zatvara fajl na kraju

				// // serijalizacija Merkle stabla
				err = merkleStablo.SerializeMerkleTree(dataOriginalRoot, dataFile)
				if err != nil {
					log.Fatalf("Greska pri serijalizaciji: %v", err)
				}

				flushInfo, err := w.CreateFlushInfo(flushedRecords)
				if err != nil {
					fmt.Println("Greska pri kreiranju FlushInfo:", err)
				} else {
					wal.SaveFlushInfoToFile(flushInfo)
					w.DeleteFullyFlushedSegments(flushInfo)
				}

			}
			w.ShowBlocks(showBlocks)

		} else if input == 3 {
			// delete
			reader := bufio.NewReader(os.Stdin)
			fmt.Print("Unesite ključ: ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)
			//fmt.Scan(&key)

			if key == tokenBucketKey {
				fmt.Println("Korisniku je zabranjena bilo kakva manipulacija sa tokenBucketom")
				continue
			}

			//fmt.Println(string(recordCache.Get(key))) // Testiranje da li radi get

			recordCache.Delete(key)

			//fmt.Println(recordCache.Get(key))

			// write to WAL
			rec := data.NewRecord(key, []byte{})
			rec.Tombstone = true
			w.AddRecord(rec)

			// write to MemTable
			flushedRecords, flush, err := memtable.Put(wal.NoZerosRecord(data.DeepCopyRecord(rec)))
			if err != nil {
				panic(err)
			}

			if len(flushedRecords) > 0 {
				for i, record := range flushedRecords {
					fmt.Printf("Element %d: %+v\n", i, record)
				}
			} else {
				fmt.Printf("Prazan niz")
			}

			if flush {
				// flushedRecords je niz pokazivaca za sstable

				numOfSSTables++
				newSSTable := "sstable_" + strconv.Itoa(numOfSSTables)
				metadataPath := "../SSTable/files/" + newSSTable + "/metadata" + strconv.Itoa(numOfSSTables)

				err2 := os.MkdirAll(metadataPath, 0755)
				if err2 != nil {
					fmt.Println("Error creating metadata folder:", err)
					return
				}

				sst := &SSTable.SSTable{
					DataSegment:     dataSeg,
					Index:           index,
					Summary:         summary,
					BlockManager:    blockManager,
					DataFilePath:    "../SSTable/files/" + newSSTable + "/data" + strconv.Itoa(numOfSSTables) + ".bin",
					IndexFilePath:   "../SSTable/files/" + newSSTable + "/index" + strconv.Itoa(numOfSSTables) + ".bin",
					SummaryFilePath: "../SSTable/files/" + newSSTable + "/summary" + strconv.Itoa(numOfSSTables) + ".bin",
					BlockSize:       BLOCK_SIZE,
				}

				sst.MakeSSTable(flushedRecords)
				sst.Index = index
				sst.Summary = summary
				sst.WriteSSTable()

				//fmt.Println("OVO JE PUTANJA")
				//fmt.Println("../SSTable/files/" + newSSTable + "/data" + strconv.Itoa(numOfSSTables) + ".bin")
				// //  ucitavanje blokova iz fajla
				dataBlocks := dataSeg.BlocksToMerkle("../SSTable/files/" + newSSTable + "/data" + strconv.Itoa(numOfSSTables) + ".bin")

				// // pravim listove stabla
				dataLeafNodes := merkleStablo.CreateLeafNodes(dataBlocks)

				//fmt.Println("OVO SU DATALEAFNODES")
				//fmt.Println(dataLeafNodes)

				//fmt.Println("JEL OVO OK ZA MERKLE STABLO")

				// // 	//  pravim originalno Merkle stablo
				dataOriginalRoot := merkleStablo.BuildMerkleTreeBottomUp(dataLeafNodes)

				//fmt.Println("OVO JE DATAORIGINALROOT")
				//fmt.Println(dataOriginalRoot)

				// // kreiram MerkleTree objekat
				//dataOriginalTree := &merkleStablo.MerkleTree{Root: dataOriginalRoot}
				//fmt.Printf("Korjen hash-a: %x\n", dataOriginalTree.Root.Hash)

				dataSerializeFileName := metadataPath + "/merkle_tree" + strconv.Itoa(numOfSSTables) + ".bin"

				// // NAPRAVI VALIDACIJU DA IDE PO FOLDERIMA KAKO TREBA DA UZIMA FAJLOVE

				// // otvaranje fajla za pisanje
				dataFile, err := os.Create(dataSerializeFileName)
				if err != nil {
					log.Fatalf("Greska pri kreiranju fajla: %v", err)
				}
				defer dataFile.Close() // Automatski zatvara fajl na kraju

				// // serijalizacija Merkle stabla
				err = merkleStablo.SerializeMerkleTree(dataOriginalRoot, dataFile)
				if err != nil {
					log.Fatalf("Greska pri serijalizaciji: %v", err)
				}

				flushInfo, err := w.CreateFlushInfo(flushedRecords)
				if err != nil {
					fmt.Println("Greska pri kreiranju FlushInfo:", err)
				} else {
					wal.SaveFlushInfoToFile(flushInfo)
					w.DeleteFullyFlushedSegments(flushInfo)
				}
			}
			w.ShowBlocks(showBlocks)

		} else if input == 4 {
			reader := bufio.NewReader(os.Stdin)
			fmt.Print("Unesite ime SStabele [npr. sstable_1]: ")
			sstableName, _ := reader.ReadString('\n')
			sstableName = strings.TrimSpace(sstableName)

			if sstableName == tokenBucketKey {
				fmt.Println("Korisniku je zabranjena bilo kakva manipulacija sa tokenBucketom")
				continue
			}

			number := strings.TrimPrefix(sstableName, "sstable_")

			filePath := "../SSTable/files/" + sstableName + "/data" + number + ".bin"
			dataFile, err := os.Open(filePath)
			if err != nil {
				fmt.Println("Ime SSTable-a nije ispravno ili fajl ne postoji:", err)
				continue
			}
			defer dataFile.Close()

			//fmt.Println("ODAVDE PRAVIM ", filePath)

			// 1. Ucitavanje blokova iz fajla
			dataBlocks := dataSeg.BlocksToMerkle(filePath)

			// pravim listove stabla
			dataLeafNodes := merkleStablo.CreateLeafNodes(dataBlocks)

			// 	//  Pravi se originalno Merkle stablo
			dataOriginalRoot := merkleStablo.BuildMerkleTreeBottomUp(dataLeafNodes)
			//fmt.Println("OVO JE KORIJENA NAPRAVLJENO SAD STABLA")
			//fmt.Printf("Korjen hash-a: %x\n", dataOriginalRoot.Hash)

			// Kreiranje MerkleTree objekta
			dataOriginalTree := &merkleStablo.MerkleTree{Root: dataOriginalRoot}
			//fmt.Printf("Korjen hash-a: %x\n", dataOriginalTree.Root.Hash)

			dataSerializeFileName := "../SSTable/files/sstable_" + number + "/metadata" + number + "/merkle_tree" + number + ".bin"
			//fmt.Println("ODAVDE CITAM OD RANIJE ", dataSerializeFileName)
			// 1. Otvaranje fajla za citanje
			metaDatafile, err := os.Open(dataSerializeFileName)
			if err != nil {
				fmt.Println("Greska pri otvaranju fajla:", err)
				return
			}
			defer metaDatafile.Close()

			root, err := merkleStablo.DeserializeMerkleTree(metaDatafile)
			if err != nil {
				fmt.Println("Greska prilikom deserijalizacije:", err)
				continue
			}

			//fmt.Printf("Korjen ucitanog stabla: %x\n", root.Hash)
			//fmt.Printf("Korjen napravljenog sad stabla: %x\n", dataOriginalTree.Root.Hash)

			if bytes.Equal(dataOriginalTree.Root.Hash, root.Hash) {
				fmt.Println("Podaci su ispravni!")
				continue
			} else {
				fmt.Println("Podaci su osteceni!")
				diffIndex := merkleStablo.CompareTrees(root, dataOriginalRoot)

				if diffIndex != -1 {
					fmt.Printf("Stabla se razlikuju na listu sa indeksom: %d\n", diffIndex)
					continue
				}

			}

		}else if input == 5{
			pairs, err := LoadKeyValuePairsFromJSON("../generated_inputs.json")
			if err != nil {
				log.Fatalf("Greška pri čitanju JSON fajla: %v", err)
			}

			for _, pair := range pairs {
				key := pair.Key
				value := []byte(pair.Value)


				if key == tokenBucketKey {
					fmt.Println("Korisniku je zabranjena bilo kakva manipulacija sa tokenBucketom")
					continue
				}

				// write to WAL
				rec := data.NewRecord(key, []byte(value))
				w.AddRecord(rec)

				// write to MemTable
				flushedRecords, flush, err := memtable.Put(wal.NoZerosRecord(data.DeepCopyRecord(rec)))
				if err != nil {
					panic(err)
				}

				if len(flushedRecords) > 0 {
					for i, record := range flushedRecords {
						fmt.Printf("Element %d: %+v\n", i, record)
					}
				} else {
					fmt.Printf("Prazan niz")
				}

				recordCache.Put(key, []byte(value)) // ovdje sam dodao upis u recordCache
				//fmt.Println("ZAPIS JE DODAT U SISTEM PRILIKOM PUT OPERACIJE I UPISAN U RECORDCACHE, U RECORDCACHE SAM DODAO KEY I VALUE ", key, string(value))

				if flush {
					// flushedRecords je niz pokazivaca za sstable
					numOfSSTables++
					newSSTable := "sstable_" + strconv.Itoa(numOfSSTables)
					sstPath := "../SSTable/files/" + newSSTable

					metadataPath := "../SSTable/files/" + newSSTable + "/metadata" + strconv.Itoa(numOfSSTables)

					err := os.MkdirAll(sstPath, 0755)
					if err != nil {
						fmt.Println("Error creating folder:", err)
						return
					}

					err2 := os.MkdirAll(metadataPath, 0755)
					if err2 != nil {
						fmt.Println("Error creating metadata folder:", err)
						return
					}

					sst := &SSTable.SSTable{
						DataSegment:     dataSeg,
						Index:           index,
						Summary:         summary,
						BlockManager:    blockManager,
						DataFilePath:    "../SSTable/files/" + newSSTable + "/data" + strconv.Itoa(numOfSSTables) + ".bin",
						IndexFilePath:   "../SSTable/files/" + newSSTable + "/index" + strconv.Itoa(numOfSSTables) + ".bin",
						SummaryFilePath: "../SSTable/files/" + newSSTable + "/summary" + strconv.Itoa(numOfSSTables) + ".bin",
						BlockSize:       BLOCK_SIZE,
					}

					sst.MakeSSTable(flushedRecords)
					sst.Index = index
					sst.Summary = summary
					sst.WriteSSTable()

					// //  ucitavanje blokova iz fajla
					dataBlocks := dataSeg.BlocksToMerkle("../SSTable/files/" + newSSTable + "/data" + strconv.Itoa(numOfSSTables) + ".bin")

					// // pravim listove stabla
					dataLeafNodes := merkleStablo.CreateLeafNodes(dataBlocks)

					// // 	//  pravim originalno Merkle stablo
					dataOriginalRoot := merkleStablo.BuildMerkleTreeBottomUp(dataLeafNodes)

					// // kreiram MerkleTree objekat
					//dataOriginalTree := &merkleStablo.MerkleTree{Root: dataOriginalRoot}
					//fmt.Printf("Korjen hash-a: %x\n", dataOriginalTree.Root.Hash)

					dataSerializeFileName := metadataPath + "/merkle_tree" + strconv.Itoa(numOfSSTables) + ".bin"

					// // otvaranje fajla za pisanje
					dataFile, err := os.Create(dataSerializeFileName)
					if err != nil {
						log.Fatalf("Greska pri kreiranju fajla: %v", err)
					}
					defer dataFile.Close() // Automatski zatvara fajl na kraju

					// // serijalizacija Merkle stabla
					err = merkleStablo.SerializeMerkleTree(dataOriginalRoot, dataFile)
					if err != nil {
						log.Fatalf("Greska pri serijalizaciji: %v", err)
					}

					flushInfo, err := w.CreateFlushInfo(flushedRecords)
					if err != nil {
						fmt.Println("Greska pri kreiranju FlushInfo:", err)
					} else {
						wal.SaveFlushInfoToFile(flushInfo)
						w.DeleteFullyFlushedSegments(flushInfo)
					}

				}
				w.ShowBlocks(showBlocks)
			}
		}

 	}
}