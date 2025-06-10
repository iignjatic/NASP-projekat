package main

import (
	"NASP-PROJEKAT/BlockCache"
	"NASP-PROJEKAT/BlockManager"
	"NASP-PROJEKAT/SSTable"
	"NASP-PROJEKAT/data"
	"NASP-PROJEKAT/memtable"
	"NASP-PROJEKAT/wal"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
)

type Config struct {
	BlockSize     uint64 `json:"BlockSize"`
	MemTableSize  uint64 `json:"MemTableSize"`
	MemTableCount uint64 `json:"MemTableCount"`
	MemTableType  string `json:"MemTableType"`
	CacheSize     uint64 `json:"CacheSize"`
	SummarySample uint64 `json:"SummarySample"`
	MaxTokens     int64  `json:"MaxTokens"`
	ResetInterval int64  `json:"ResetInterval"`
}

func main() {
	//DEFAULT VRIJEDNOSTI KONFIGURACIJE
	var BLOCK_SIZE uint64 = 70
	var MEMTABLE_SIZE uint64 = 5
	var MEMTABLE_COUNT uint64 = 1
	var MEMTABLE_TYPE string = "hashmap"
	//var CACHE_SIZE uint32 = 10
	var SUMMARY_SAMPLE uint64 = 5
	//var MAX_TOKENS int64 = 10
	//var RESET_INTERVAL int64 = 30

	configFile, err := os.Open("../config.json")
	if err != nil {
		log.Fatalf("Failed to open JSON file: %v", err)
	}
	defer configFile.Close()

	configBytes, err := ioutil.ReadAll(configFile)
	if err != nil {
		log.Fatalf("Failed to open JSON file: %v", err)
	}

	var config Config
	err = json.Unmarshal(configBytes, &config)
	if err != nil {
		log.Fatalf("Failed to unmarshal JSON: %v", err)
	} else {
		BLOCK_SIZE = uint64(config.BlockSize)
		MEMTABLE_SIZE = uint64(config.MemTableSize)
		MEMTABLE_COUNT = uint64(config.MemTableCount)
		MEMTABLE_TYPE = config.MemTableType
		//CACHE_SIZE = uint32(config.CacheSize)
		SUMMARY_SAMPLE = uint64(config.SummarySample)
		//MAX_TOKENS = int64(config.MaxTokens)
		//RESET_INTERVAL = int64(config.ResetInterval)
	}

	//PRISTUPANJE KONFIGURACIONIM ATRIBUTIMA
	//config.BlockSize
	//config.MemTableSize
	//config.CacheSize ...

	/* FORMIRANJE STRUKTURA */

	/*
		- memtable
		- wal
		- cache


	*/

	//recordCache := lruCache.MakeLRUCache(int(CACHE_SIZE))

	memtable, err := memtable.CreateMemtableManager(MEMTABLE_TYPE, int(MEMTABLE_COUNT), int(MEMTABLE_SIZE))
	if err != nil {
		panic(err)
	}

	w := wal.NewWal()

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

	var input uint32
	var key string
	var value []byte

	// var fileForIntegrity string
	// const tokenBucketKey = "token_bucket"

	var tempRecords []*data.Record
	recs, err := w.ReadAllSegments()
	if err != nil {
		fmt.Printf("Segment deserialization failed: %v\n", err)
		return
	}

	_, err = memtable.LoadFromWal(recs)
	if err != nil {
		fmt.Println("Greška pri učitavanju podataka u MemTable:", err)
		return
	}

	// found := false

	// if record, _ := memtable.Get(tokenBucketKey); record != nil {
	// 	fmt.Println("Zapis je pronadjen : ", string(value))
	// 	found = true
	// } else if value = recordCache.Get(tokenBucketKey); value != nil && found == false {
	// 	fmt.Println("Zapis je pronadjen : ", string(value))
	// 	found = true
	// } else if found == false {
	// 	for i := numOfSSTables; i > 0; i-- { //prolazak kroz sve sstabele
	// 		// Kreiranje SSTable-a
	// 		sst := &SSTable.SSTable{
	// 			DataSegment:     dataSeg,
	// 			Index:           index,
	// 			Summary:         summary,
	// 			BlockManager:    blockManager,
	// 			DataFilePath:    "../SSTable/files/sstable_" + strconv.Itoa(i) + "/data" + strconv.Itoa(i) + ".bin",
	// 			IndexFilePath:   "../SSTable/files/sstable_" + strconv.Itoa(i) + "/index" + strconv.Itoa(i) + ".bin",
	// 			SummaryFilePath: "../SSTable/files/sstable_" + strconv.Itoa(i) + "/summary" + strconv.Itoa(i) + ".bin",
	// 			BlockSize:       BLOCK_SIZE,
	// 		}
	// 		summarySize, err := os.Stat(sst.SummaryFilePath)
	// 		if err != nil {
	// 			panic(err)
	// 		}
	// 		indexSize, err := os.Stat(sst.IndexFilePath)
	// 		if err != nil {
	// 			panic(err)
	// 		}
	// 		dataSize, err := os.Stat(sst.DataFilePath)
	// 		if err != nil {
	// 			panic(err)
	// 		}
	// 		sst.Summary.SegmentSize = uint64(summarySize.Size()) / BLOCK_SIZE
	// 		sst.Index.SegmentSize = uint64(indexSize.Size()) / BLOCK_SIZE
	// 		sst.DataSegment.SegmentSize = uint64(dataSize.Size()) / BLOCK_SIZE
	// 		value = sst.Get(tokenBucketKey)
	// 		if value == nil {
	// 			continue
	// 		} else if len(value) == 0 {
	// 			fmt.Println("Zapis je obrisan")
	// 			break
	// 		} else {
	// 			fmt.Println("Zapis je pronadjen : ", string(value))
	// 			recordCache.Put(tokenBucketKey, value)
	// 			found = true
	// 			//update CACHE!!!!!!!!!!!!!!!!!!
	// 			break
	// 		}
	// 	}
	// }

	// if value == nil {
	// 	fmt.Println("Zapis nije pronadjen")
	// }

	// var tb *tokenBucket.TokenBucket

	// if !found {
	// 	tb = tokenBucket.NewTokenBucket(MAX_TOKENS, RESET_INTERVAL)
	// 	tokenBucketState, err := tb.SerializeState()
	// 	if err != nil {
	// 		log.Fatalf("Greska prilikom serijalizacije tokenBucket-a: %v", err)
	// 	}

	// 	// put

	// 	// write to WAL
	// 	rec := data.NewRecord(tokenBucketKey, tokenBucketState)
	// 	w.AddRecord(rec)

	// 	// write to MemTable
	// 	flushedRecords, flush, err := memtable.Put(rec)
	// 	fmt.Println(flush)

	// 	if len(flushedRecords) > 0 {
	// 		for i, record := range flushedRecords {
	// 			fmt.Printf("Element %d: %+v\n", i, record)
	// 		}
	// 	} else {
	// 		fmt.Printf("Prazan niz")
	// 	}

	// 	if err != nil {
	// 		panic(err)
	// 	}

	// 	recordCache.Put(tokenBucketKey, tokenBucketState) // ovdje sam dodao upis u recordCache

	// 	if flush {
	// 		wal.WriteNumbersToFile("../wal/walhelper.txt", 0, wal.CalculateRecordsSize(tempRecords))
	// 		// flushedRecords je niz pokazivaca za sstable
	// 		numOfSSTables++
	// 		newSSTable := "sstable_" + strconv.Itoa(numOfSSTables)
	// 		sstPath := "../SSTable/files/" + newSSTable

	// 		err := os.MkdirAll(sstPath, 0755)
	// 		if err != nil {
	// 			fmt.Println("Error creating folder:", err)
	// 			return
	// 		}

	// 		sst := &SSTable.SSTable{
	// 			DataSegment:     dataSeg,
	// 			Index:           index,
	// 			Summary:         summary,
	// 			BlockManager:    blockManager,
	// 			DataFilePath:    "../SSTable/files/" + newSSTable + "/data" + strconv.Itoa(numOfSSTables) + ".bin",
	// 			IndexFilePath:   "../SSTable/files/" + newSSTable + "/index" + strconv.Itoa(numOfSSTables) + ".bin",
	// 			SummaryFilePath: "../SSTable/files/" + newSSTable + "/summary" + strconv.Itoa(numOfSSTables) + ".bin",
	// 			BlockSize:       BLOCK_SIZE,
	// 		}

	// 		sst.MakeSSTable(flushedRecords)
	// 		sst.Index = index
	// 		sst.Summary = summary
	// 		sst.WriteSSTable()

	// 		dataFileName := "../SSTable/files/data" + strconv.Itoa(numOfSSTables) + ".bin"

	// 		//  ucitavanje blokova iz fajla
	// 		dataBlocks := dataSeg.BlocksToMerkle(dataFileName)

	// 		// pravim listove stabla
	// 		dataLeafNodes := merkleStablo.CreateLeafNodes(dataBlocks)

	// 		// 	//  pravim originalno Merkle stablo
	// 		dataOriginalRoot := merkleStablo.BuildMerkleTreeBottomUp(dataLeafNodes)

	// 		// kreiram MerkleTree objekat
	// 		dataOriginalTree := &merkleStablo.MerkleTree{Root: dataOriginalRoot}
	// 		fmt.Printf("Korjen hash-a: %x\n", dataOriginalTree.Root.Hash)

	// 		dataSerializeFileName := "../merkleStablo/files/dataMerkleTree" + strconv.Itoa(numOfSSTables) + ".bin"

	// 		// otvaranje fajla za pisanje
	// 		dataFile, err := os.Create(dataSerializeFileName)
	// 		if err != nil {
	// 			log.Fatalf("Greska pri kreiranju fajla: %v", err)
	// 		}
	// 		defer dataFile.Close() // Automatski zatvara fajl na kraju

	// 		// serijalizacija Merkle stabla
	// 		err = merkleStablo.SerializeMerkleTree(dataOriginalRoot, dataFile)
	// 		if err != nil {
	// 			log.Fatalf("Greska pri serijalizaciji: %v", err)
	// 		}

	// 		//err := w.DeleteFullyFlushedSegments(flushedRecords)
	// 		// if err != nil {
	// 		// 	panic(err)
	// 		// }
	// 		// tempRecords = nil
	// 		// wal.WriteNumbersToFile("../wal/walhelper.txt", 0, 0)
	// 	}
	// 	for i := 0; i < len(w.Segments); i++ {
	// 		fmt.Printf("\n----------------------Segment %d----------------------\n", w.Segments[i].ID)
	// 		w.Segments[i].PrintBlocks()
	// 	}
	// 	fmt.Print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	// 	fmt.Print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	// 	defragmentedRecords, err := w.ReadAllSegments()
	// 	if err != nil {
	// 		fmt.Printf("Segment deserialization failed: %v\n", err)
	// 		return
	// 	}
	// 	for i := 0; i < len(defragmentedRecords); i++ {
	// 		fmt.Println(defragmentedRecords[i])
	// 	}

	// }

	for {
		fmt.Println(" * KEY - VALUE ENGINE * ")
		fmt.Println("Izaberite opciju: ")
		fmt.Println("1. GET [ key ] ")
		fmt.Println("2. PUT [ key, value] ")
		fmt.Println("3. DELETE [ key ]")
		//fmt.Println("4. PROVJERA INTEGRITETA PODATAKA [ naziv fajla ]")
		//fmt.Println("5. IZLAZ")

		fmt.Scan(&input)
		//input = 1

		if input == 1 {
			//GET operacija

			fmt.Scan(&key)
			//key = "key2"

			// if key == tokenBucketKey {
			// 	continue
			// }

			if record, _ := memtable.Get(key); record != nil {
				fmt.Println("Zapis je pronadjen : ", string(value))
				continue
			}

			// else if SEARCHCACHE != nil
			// 	continue

			// if value = recordCache.Get(key); value != nil {
			// 	fmt.Println("Zapis je pronadjen : ", string(value))
			// 	continue
			// }

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
					//recordCache.Put(key, value)
					//update CACHE!!!!!!!!!!!!!!!!!!
					break
				}

			}
			if value == nil {
				fmt.Println("Zapis nije pronadjen")
			}

			// get operacija da dobavim tokenBucket

			// found = false
			// var tokenBucketStateValue []byte

			// if record, _ := memtable.Get(tokenBucketKey); record != nil {
			// 	fmt.Println("Zapis je pronadjen : ", string(value))
			// 	//found = true
			// 	//tokenBucketStateValue = value
			// } else if value = recordCache.Get(tokenBucketKey); value != nil && found == false {
			// 	fmt.Println("Zapis je pronadjen : ", string(value))
			// 	found = true
			// 	tokenBucketStateValue = value
			// } else if found == false {
			// 	for i := numOfSSTables; i > 0; i-- { //prolazak kroz sve sstabele
			// 		// Kreiranje SSTable-a
			// 		sst := &SSTable.SSTable{
			// 			DataSegment:     dataSeg,
			// 			Index:           index,
			// 			Summary:         summary,
			// 			BlockManager:    blockManager,
			// 			DataFilePath:    "../SSTable/files/sstable_" + strconv.Itoa(i) + "/data" + strconv.Itoa(i) + ".bin",
			// 			IndexFilePath:   "../SSTable/files/sstable_" + strconv.Itoa(i) + "/index" + strconv.Itoa(i) + ".bin",
			// 			SummaryFilePath: "../SSTable/files/sstable_" + strconv.Itoa(i) + "/summary" + strconv.Itoa(i) + ".bin",
			// 			BlockSize:       BLOCK_SIZE,
			// 		}
			// 		summarySize, err := os.Stat(sst.SummaryFilePath)
			// 		if err != nil {
			// 			panic(err)
			// 		}
			// 		indexSize, err := os.Stat(sst.IndexFilePath)
			// 		if err != nil {
			// 			panic(err)
			// 		}
			// 		dataSize, err := os.Stat(sst.DataFilePath)
			// 		if err != nil {
			// 			panic(err)
			// 		}
			// 		sst.Summary.SegmentSize = uint64(summarySize.Size()) / BLOCK_SIZE
			// 		sst.Index.SegmentSize = uint64(indexSize.Size()) / BLOCK_SIZE
			// 		sst.DataSegment.SegmentSize = uint64(dataSize.Size()) / BLOCK_SIZE
			// 		value = sst.Get(tokenBucketKey)
			// 		tokenBucketStateValue = value
			// 		if value == nil {
			// 			continue
			// 		} else if len(value) == 0 {
			// 			fmt.Println("Zapis je obrisan")
			// 			break
			// 		} else {
			// 			fmt.Println("Zapis je pronadjen : ", string(value))
			// 			recordCache.Put(tokenBucketKey, value)
			// 			found = true
			// 			tokenBucketStateValue = value
			// 			//update CACHE!!!!!!!!!!!!!!!!!!
			// 			break
			// 		}
			// 	}
			// }

			// if value == nil {
			// 	fmt.Println("Zapis nije pronadjen")
			// }

			// // ako sam nasao tokenBucket u sistemu, mijenjam njegovo stanje i ponovo ga serijalizujem
			// if found {
			// 	err := tb.DeserializeState(tokenBucketStateValue)
			// 	if err != nil {
			// 		// Ako deserijalizacija nije uspjela, napravi novi TokenBucket
			// 		fmt.Println("Greška pri deserijalizaciji TokenBucket-a")
			// 		continue
			// 	}
			// 	tb.GetTokens()
			// 	tb.SerializeState()
			// }

		} else if input == 2 {
			// put
			fmt.Scan(&key, &value)

			// if key == tokenBucketKey {
			// 	continue
			// }

			// write to WAL
			rec := data.NewRecord(key, []byte(value))
			w.AddRecord(rec)

			// write to MemTable
			flushedRecords, flush, err := memtable.Put(rec)
			fmt.Println(flush)

			if len(flushedRecords) > 0 {
				for i, record := range flushedRecords {
					fmt.Printf("Element %d: %+v\n", i, record)
				}
			} else {
				fmt.Printf("Prazan niz")
			}

			if err != nil {
				panic(err)
			}

			//	recordCache.Put(key, []byte(value)) // ovdje sam dodao upis u recordCache

			if flush {
				wal.WriteNumbersToFile("../wal/walhelper.txt", 0, wal.CalculateRecordsSize(tempRecords))
				// flushedRecords je niz pokazivaca za sstable
				numOfSSTables++
				newSSTable := "sstable_" + strconv.Itoa(numOfSSTables)
				sstPath := "../SSTable/files/" + newSSTable

				//metadataPath := "../SSTable/files/" + newSSTable + "/metadata"

				err := os.MkdirAll(sstPath, 0755)
				if err != nil {
					fmt.Println("Error creating folder:", err)
					return
				}

				// err2 := os.MkdirAll(metadataPath, 0755)
				// if err2 != nil {
				// 	fmt.Println("Error creating metadata folder:", err)
				// 	return
				// }

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
				// dataBlocks := dataSeg.BlocksToMerkle(metadataPath)

				// // pravim listove stabla
				// dataLeafNodes := merkleStablo.CreateLeafNodes(dataBlocks)

				// // 	//  pravim originalno Merkle stablo
				// dataOriginalRoot := merkleStablo.BuildMerkleTreeBottomUp(dataLeafNodes)

				// // kreiram MerkleTree objekat
				// dataOriginalTree := &merkleStablo.MerkleTree{Root: dataOriginalRoot}
				// fmt.Printf("Korjen hash-a: %x\n", dataOriginalTree.Root.Hash)

				// dataSerializeFileName := metadataPath + ".bin"

				// NAPRAVI VALIDACIJU DA IDE PO FOLDERIMA KAKO TREBA DA UZIMA FAJLOVE

				// otvaranje fajla za pisanje
				// dataFile, err := os.Create(dataSerializeFileName)
				// if err != nil {
				// 	log.Fatalf("Greska pri kreiranju fajla: %v", err)
				// }
				// defer dataFile.Close() // Automatski zatvara fajl na kraju

				// // serijalizacija Merkle stabla
				// err = merkleStablo.SerializeMerkleTree(dataOriginalRoot, dataFile)
				// if err != nil {
				// 	log.Fatalf("Greska pri serijalizaciji: %v", err)
				// }

				// err := w.DeleteFullyFlushedSegments(flushedRecords)
				// if err != nil {
				// 	panic(err)
				// }
				// tempRecords = nil
				// wal.WriteNumbersToFile("../wal/walhelper.txt", 0, 0)
			}
			for i := 0; i < len(w.Segments); i++ {
				fmt.Printf("\n----------------------Segment %d----------------------\n", w.Segments[i].ID)
				w.Segments[i].PrintBlocks()
			}
			fmt.Print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
			fmt.Print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
			defragmentedRecords, err := w.ReadAllSegments()
			if err != nil {
				fmt.Printf("Segment deserialization failed: %v\n", err)
				return
			}
			for i := 0; i < len(defragmentedRecords); i++ {
				fmt.Println(defragmentedRecords[i])
			}

			// get operacija da dobavim tokenBucket

			// found = false
			// var tokenBucketStateValue []byte

			// if record, _ := memtable.Get(tokenBucketKey); record != nil {
			// 	fmt.Println("Zapis je pronadjen : ", string(value))
			// 	found = true
			// 	tokenBucketStateValue = value
			// } else if value = recordCache.Get(tokenBucketKey); value != nil && found == false {
			// 	fmt.Println("Zapis je pronadjen : ", string(value))
			// 	found = true
			// 	tokenBucketStateValue = value
			// } else if found == false {
			// 	for i := numOfSSTables; i > 0; i-- { //prolazak kroz sve sstabele
			// 		// Kreiranje SSTable-a
			// 		sst := &SSTable.SSTable{
			// 			DataSegment:     dataSeg,
			// 			Index:           index,
			// 			Summary:         summary,
			// 			BlockManager:    blockManager,
			// 			DataFilePath:    "../SSTable/files/sstable_" + strconv.Itoa(i) + "/data" + strconv.Itoa(i) + ".bin",
			// 			IndexFilePath:   "../SSTable/files/sstable_" + strconv.Itoa(i) + "/index" + strconv.Itoa(i) + ".bin",
			// 			SummaryFilePath: "../SSTable/files/sstable_" + strconv.Itoa(i) + "/summary" + strconv.Itoa(i) + ".bin",
			// 			BlockSize:       BLOCK_SIZE,
			// 		}
			// 		summarySize, err := os.Stat(sst.SummaryFilePath)
			// 		if err != nil {
			// 			panic(err)
			// 		}
			// 		indexSize, err := os.Stat(sst.IndexFilePath)
			// 		if err != nil {
			// 			panic(err)
			// 		}
			// 		dataSize, err := os.Stat(sst.DataFilePath)
			// 		if err != nil {
			// 			panic(err)
			// 		}
			// 		sst.Summary.SegmentSize = uint64(summarySize.Size()) / BLOCK_SIZE
			// 		sst.Index.SegmentSize = uint64(indexSize.Size()) / BLOCK_SIZE
			// 		sst.DataSegment.SegmentSize = uint64(dataSize.Size()) / BLOCK_SIZE
			// 		value = sst.Get(tokenBucketKey)
			// 		tokenBucketStateValue = value
			// 		if value == nil {
			// 			continue
			// 		} else if len(value) == 0 {
			// 			fmt.Println("Zapis je obrisan")
			// 			break
			// 		} else {
			// 			fmt.Println("Zapis je pronadjen : ", string(value))
			// 			recordCache.Put(tokenBucketKey, value)
			// 			found = true
			// 			tokenBucketStateValue = value
			// 			//update CACHE!!!!!!!!!!!!!!!!!!
			// 			break
			// 		}
			// 	}
			// }

			// if value == nil {
			// 	fmt.Println("Zapis nije pronadjen")
			// }

			// // ako sam nasao tokenBucket u sistemu, mijenjam njegovo stanje i ponovo ga serijalizujem
			// if found {
			// 	err := tb.DeserializeState(tokenBucketStateValue)
			// 	if err != nil {
			// 		// Ako deserijalizacija nije uspjela, napravi novi TokenBucket
			// 		fmt.Println("Greška pri deserijalizaciji TokenBucket-a")
			// 		continue
			// 	}
			// 	tb.GetTokens()
			// 	tb.SerializeState()
			// }

		} else if input == 3 {

			// 		//DELETE OPERACIJA
			// 		fmt.Scan(&key)

			// if key == tokenBucketKey {
			// 	continue
			// }

			// fmt.Println(string(recordCache.Get(key))) // Testiranje da li radi get

			// recordCache.Delete(key)

			// fmt.Println(recordCache.Get(key))

			// 		//writeToWAL

			// 		flushedRecords, flush, err := memtable.Delete(record)
			// 		if err != nil {
			// 			panic(err)
			// 		} else if flush {
			// 			// flushedRecords je niz pokazivaca za sstable
			// 			numOfSSTables++
			// 			sst := &SSTable.SSTable{
			// 				DataSegment:     dataSeg,
			// 				Index:           index,
			// 				Summary:         summary,
			// 				BlockManager:    blockManager,
			// 				DataFilePath:    "../SSTable/files/data" + strconv.Itoa(numOfSSTables) + ".bin",
			// 				IndexFilePath:   "../SSTable/files/index" + strconv.Itoa(numOfSSTables) + ".bin",
			// 				SummaryFilePath: "../SSTable/files/summary" + strconv.Itoa(numOfSSTables) + ".bin",
			// 			}

			// 			sst.MakeSSTable(flushedRecords)

			// 			sst.Index = index
			// 			sst.Summary = summary

			// 			sst.WriteSSTable()
			// 			// RECIMO DA JE POSLATO NA SSTABLE
			// 			wal.DeleteSegments(segmentsToDelete, true)
			// 		}

			// get operacija da dobavim tokenBucket

			// found = false
			// var tokenBucketStateValue []byte

			// if record, _ := memtable.Get(tokenBucketKey); record != nil {
			// 	fmt.Println("Zapis je pronadjen : ", string(value))
			// 	found = true
			// 	tokenBucketStateValue = value
			// } else if value = recordCache.Get(tokenBucketKey); value != nil && found == false {
			// 	fmt.Println("Zapis je pronadjen : ", string(value))
			// 	found = true
			// 	tokenBucketStateValue = value
			// } else if found == false {
			// 	for i := numOfSSTables; i > 0; i-- { //prolazak kroz sve sstabele
			// 		// Kreiranje SSTable-a
			// 		sst := &SSTable.SSTable{
			// 			DataSegment:     dataSeg,
			// 			Index:           index,
			// 			Summary:         summary,
			// 			BlockManager:    blockManager,
			// 			DataFilePath:    "../SSTable/files/sstable_" + strconv.Itoa(i) + "/data" + strconv.Itoa(i) + ".bin",
			// 			IndexFilePath:   "../SSTable/files/sstable_" + strconv.Itoa(i) + "/index" + strconv.Itoa(i) + ".bin",
			// 			SummaryFilePath: "../SSTable/files/sstable_" + strconv.Itoa(i) + "/summary" + strconv.Itoa(i) + ".bin",
			// 			BlockSize:       BLOCK_SIZE,
			// 		}
			// 		summarySize, err := os.Stat(sst.SummaryFilePath)
			// 		if err != nil {
			// 			panic(err)
			// 		}
			// 		indexSize, err := os.Stat(sst.IndexFilePath)
			// 		if err != nil {
			// 			panic(err)
			// 		}
			// 		dataSize, err := os.Stat(sst.DataFilePath)
			// 		if err != nil {
			// 			panic(err)
			// 		}
			// 		sst.Summary.SegmentSize = uint64(summarySize.Size()) / BLOCK_SIZE
			// 		sst.Index.SegmentSize = uint64(indexSize.Size()) / BLOCK_SIZE
			// 		sst.DataSegment.SegmentSize = uint64(dataSize.Size()) / BLOCK_SIZE
			// 		value = sst.Get(tokenBucketKey)
			// 		tokenBucketStateValue = value
			// 		if value == nil {
			// 			continue
			// 		} else if len(value) == 0 {
			// 			fmt.Println("Zapis je obrisan")
			// 			break
			// 		} else {
			// 			fmt.Println("Zapis je pronadjen : ", string(value))
			// 			recordCache.Put(tokenBucketKey, value)
			// 			found = true
			// 			tokenBucketStateValue = value
			// 			//update CACHE!!!!!!!!!!!!!!!!!!
			// 			break
			// 		}
			// 	}
			// }

			// if value == nil {
			// 	fmt.Println("Zapis nije pronadjen")
			// }

			// // ako sam nasao tokenBucket u sistemu, mijenjam njegovo stanje i ponovo ga serijalizujem
			// if found {
			// 	err := tb.DeserializeState(tokenBucketStateValue)
			// 	if err != nil {
			// 		// Ako deserijalizacija nije uspjela, napravi novi TokenBucket
			// 		fmt.Println("Greška pri deserijalizaciji TokenBucket-a")
			// 		continue
			// 	}
			// 	tb.GetTokens()
			// 	tb.SerializeState()
			// }

		} else if input == 4 {
			// fmt.Scan(&fileForIntegrity)

			// if key == tokenBucketKey {
			// 	continue
			// }

			// filePath := "../SSTable/files/" + fileForIntegrity
			// file, err := os.Open(filePath)
			// if err != nil {
			// 	fmt.Println("Fajl koji ste unijeli ne postoji")
			// 	continue
			// }
			// defer file.Close()

			// re := regexp.MustCompile(`\d+`)
			// numbers := re.FindAllString(fileForIntegrity, -1)

			// // 1. Ucitavanje blokova iz fajla
			// dataBlocks := dataSeg.BlocksToMerkle(filePath)

			// // pravim listove stabla
			// dataLeafNodes := merkleStablo.CreateLeafNodes(dataBlocks)

			// // 	//  Pravi se originalno Merkle stablo
			// dataOriginalRoot := merkleStablo.BuildMerkleTreeBottomUp(dataLeafNodes)

			// // Kreiranje MerkleTree objekta
			// dataOriginalTree := &merkleStablo.MerkleTree{Root: dataOriginalRoot}
			// fmt.Printf("Korjen hash-a: %x\n", dataOriginalTree.Root.Hash)

			// dataSerializeFileName := "../SSTable/files/sstable_" + numbers[0] + "" + ".bin"
			// // 1. Otvaranje fajla za citanje
			// file, err = os.Open(dataSerializeFileName)
			// if err != nil {
			// 	fmt.Println("Greska pri otvaranju fajla:", err)
			// 	return
			// }
			// defer file.Close()

			// root, err := merkleStablo.DeserializeMerkleTree(file)
			// if err != nil {
			// 	fmt.Println("Greska prilikom deserijalizacije:", err)
			// 	continue
			// }

			// fmt.Printf("Korjen ucitanog stabla: %x\n", root.Hash)

			// if bytes.Equal(dataOriginalTree.Root.Hash, root.Hash) {
			// 	fmt.Println("Podaci su ispravni!")
			// } else {
			// 	fmt.Println("Podaci su osteceni!")
			// 	diffIndex := merkleStablo.CompareTrees(root, dataOriginalTree.Root)
			// 	if diffIndex == -1 {
			// 		fmt.Println("Stabla su identicna.")
			// 	} else {
			// 		fmt.Printf("Stabla se razlikuju na listu sa indeksom: %d\n", diffIndex)
			// 	}

			// }

			// // get operacija da dobavim tokenBucket

			// found = false
			// var tokenBucketStateValue []byte

			// if record, _ := memtable.Get(tokenBucketKey); record != nil {
			// 	fmt.Println("Zapis je pronadjen : ", string(value))
			// 	found = true
			// 	tokenBucketStateValue = value
			// } else if value = recordCache.Get(tokenBucketKey); value != nil && found == false {
			// 	fmt.Println("Zapis je pronadjen : ", string(value))
			// 	found = true
			// 	tokenBucketStateValue = value
			// } else if found == false {
			// 	for i := numOfSSTables; i > 0; i-- { //prolazak kroz sve sstabele
			// 		// Kreiranje SSTable-a
			// 		sst := &SSTable.SSTable{
			// 			DataSegment:     dataSeg,
			// 			Index:           index,
			// 			Summary:         summary,
			// 			BlockManager:    blockManager,
			// 			DataFilePath:    "../SSTable/files/sstable_" + strconv.Itoa(i) + "/data" + strconv.Itoa(i) + ".bin",
			// 			IndexFilePath:   "../SSTable/files/sstable_" + strconv.Itoa(i) + "/index" + strconv.Itoa(i) + ".bin",
			// 			SummaryFilePath: "../SSTable/files/sstable_" + strconv.Itoa(i) + "/summary" + strconv.Itoa(i) + ".bin",
			// 			BlockSize:       BLOCK_SIZE,
			// 		}
			// 		summarySize, err := os.Stat(sst.SummaryFilePath)
			// 		if err != nil {
			// 			panic(err)
			// 		}
			// 		indexSize, err := os.Stat(sst.IndexFilePath)
			// 		if err != nil {
			// 			panic(err)
			// 		}
			// 		dataSize, err := os.Stat(sst.DataFilePath)
			// 		if err != nil {
			// 			panic(err)
			// 		}
			// 		sst.Summary.SegmentSize = uint64(summarySize.Size()) / BLOCK_SIZE
			// 		sst.Index.SegmentSize = uint64(indexSize.Size()) / BLOCK_SIZE
			// 		sst.DataSegment.SegmentSize = uint64(dataSize.Size()) / BLOCK_SIZE
			// 		value = sst.Get(tokenBucketKey)
			// 		tokenBucketStateValue = value
			// 		if value == nil {
			// 			continue
			// 		} else if len(value) == 0 {
			// 			fmt.Println("Zapis je obrisan")
			// 			break
			// 		} else {
			// 			fmt.Println("Zapis je pronadjen : ", string(value))
			// 			recordCache.Put(tokenBucketKey, value)
			// 			found = true
			// 			tokenBucketStateValue = value
			// 			//update CACHE!!!!!!!!!!!!!!!!!!
			// 			break
			// 		}
			// 	}
			// }

			// if value == nil {
			// 	fmt.Println("Zapis nije pronadjen")
			// }

			// // ako sam nasao tokenBucket u sistemu, mijenjam njegovo stanje i ponovo ga serijalizujem
			// if found {
			// 	err := tb.DeserializeState(tokenBucketStateValue)
			// 	if err != nil {
			// 		// Ako deserijalizacija nije uspjela, napravi novi TokenBucket
			// 		fmt.Println("Greška pri deserijalizaciji TokenBucket-a")
			// 		continue
			// 	}
			// 	tb.GetTokens()
			// 	tb.SerializeState()
			// }
			continue // ovo je ovjde privremeno

		} else if input == 5 {
			break
		}
	}

}
