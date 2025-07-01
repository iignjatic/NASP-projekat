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
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
)

type Config struct {
	BlockSize     uint64 `json:"BlockSize"`
	BlocksPerSegment uint64 `json:"BlocksPerSegment"`
	MemTableSize  uint64 `json:"MemTableSize"`
	MemTableCount uint64 `json:"MemTableCount"`
	MemTableType  string `json:"MemTableType"`
	CacheSize     uint64 `json:"CacheSize"`
	SummarySample uint64 `json:"SummarySample"`
	MaxTokens     int64  `json:"MaxTokens"`
	ResetInterval int64  `json:"ResetInterval"`
}

func  LoadConfig(path string) (Config, error) {
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
	//var CACHE_SIZE uint32 = 10
	var SUMMARY_SAMPLE uint64 = 5
	// var MAX_TOKENS int64 = 10
	// var RESET_INTERVAL int64 = 30

	config, err := LoadConfig("../config.json")
	if err != nil {
		log.Fatalf("Failed to unmarshal JSON: %v", err)
	} else {
		BLOCK_SIZE = uint64(config.BlockSize)
		BLOCKS_PER_SEGMENT = uint64(config.BlocksPerSegment)
		MEMTABLE_SIZE = uint64(config.MemTableSize)
		MEMTABLE_COUNT = uint64(config.MemTableCount)
		MEMTABLE_TYPE = config.MemTableType
		//CACHE_SIZE = uint32(config.CacheSize)
		SUMMARY_SAMPLE = uint64(config.SummarySample)
	}

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
	}
	dataSeg := &SSTable.DataSegment{
		BlockManager: *blockManager,
	}
	index := &SSTable.Index{}
	summary := &SSTable.Summary{
		Sample: SUMMARY_SAMPLE,
	}

	files, _ := ioutil.ReadDir("../SSTable/files")
	numOfSSTables := len(files) / 3

	var input uint32
	var key string
	var value []byte


	w := wal.NewWal(BLOCK_SIZE, BLOCKS_PER_SEGMENT)

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

	for {
		fmt.Println(" * KEY - VALUE ENGINE * ")
		fmt.Println("Izaberite opciju: ")
		fmt.Println("1. GET [ key ] ")
		fmt.Println("2. PUT [ key, value] ")
		fmt.Println("3. DELETE [ key ]")

		fmt.Scan(&input)
		//input = 1

		if input == 1 {
			//GET operacija

			fmt.Scan(&key)
			//key = "key2"

			if record, _ := memtable.Get(key); record != nil {
				fmt.Println("Zapis je pronadjen : ", string(value))
				continue
			}

			// else if SEARCHCACHE != nil
			// 	continue

			for i := numOfSSTables; i > 0; i-- { //prolazak kroz sve sstabele
				// Kreiranje SSTable-a
				sst := &SSTable.SSTable{
					DataSegment:     dataSeg,
					Index:           index,
					Summary:         summary,
					BlockManager:    blockManager,
					DataFilePath:    "../SSTable/files/data" + strconv.Itoa(i) + ".bin",
					IndexFilePath:   "../SSTable/files/index" + strconv.Itoa(i) + ".bin",
					SummaryFilePath: "../SSTable/files/summary" + strconv.Itoa(i) + ".bin",
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
					//update CACHE!!!!!!!!!!!!!!!!!!
					break
				}

			}
			if value == nil {
				fmt.Println("Zapis nije pronadjen")
			}

		} else if input == 2 {
			// put
			fmt.Scan(&key, &value)

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
			
			if flush {
				// flushedRecords je niz pokazivaca za sstable
				numOfSSTables++
				sst := &SSTable.SSTable{
					DataSegment:     dataSeg,
					Index:           index,
					Summary:         summary,
					BlockManager:    blockManager,
					DataFilePath:    "../SSTable/files/data" + strconv.Itoa(numOfSSTables) + ".bin",
					IndexFilePath:   "../SSTable/files/index" + strconv.Itoa(numOfSSTables) + ".bin",
					SummaryFilePath: "../SSTable/files/summary" + strconv.Itoa(numOfSSTables) + ".bin",
					BlockSize:       BLOCK_SIZE,
				}

				sst.MakeSSTable(flushedRecords)
				sst.Index = index
				sst.Summary = summary
				sst.WriteSSTable()

				flushInfo, err := w.CreateFlushInfo(flushedRecords)
				if err != nil {
					fmt.Println("Greska pri kreiranju FlushInfo:", err)
				} else {
					wal.SaveFlushInfoToFile(flushInfo)
					w.DeleteFullyFlushedSegments(flushInfo)
				}
			}
			for i:=0; i<len(w.Segments);i++ {
				fmt.Printf("\n----------------------Segment %d----------------------\n", w.Segments[i].ID)
				w.Segments[i].PrintBlocks()
			}
			fmt.Print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
			fmt.Print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
			defragmentedRecords, err := w.ReadAllSegmentsCP(false)
			if err != nil {
				fmt.Printf("Segment deserialization failed: %v\n", err)
				return
			}
			for i:=0; i<len(defragmentedRecords); i++ {
				fmt.Println(defragmentedRecords[i])
			}
		} else if input == 3 {
			// delete
			fmt.Scan(&key)

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
				sst := &SSTable.SSTable{
					DataSegment:     dataSeg,
					Index:           index,
					Summary:         summary,
					BlockManager:    blockManager,
					DataFilePath:    "../SSTable/files/data" + strconv.Itoa(numOfSSTables) + ".bin",
					IndexFilePath:   "../SSTable/files/index" + strconv.Itoa(numOfSSTables) + ".bin",
					SummaryFilePath: "../SSTable/files/summary" + strconv.Itoa(numOfSSTables) + ".bin",
					BlockSize:       BLOCK_SIZE,
				}

				sst.MakeSSTable(flushedRecords)
				sst.Index = index
				sst.Summary = summary
				sst.WriteSSTable()

				flushInfo, err := w.CreateFlushInfo(flushedRecords)
				if err != nil {
					fmt.Println("Greska pri kreiranju FlushInfo:", err)
				} else {
					wal.SaveFlushInfoToFile(flushInfo)
					w.DeleteFullyFlushedSegments(flushInfo)
				}
			}
			for i:=0; i<len(w.Segments);i++ {
				fmt.Printf("\n----------------------Segment %d----------------------\n", w.Segments[i].ID)
				w.Segments[i].PrintBlocks()
			}
			fmt.Print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
			fmt.Print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
			defragmentedRecords, err := w.ReadAllSegmentsCP(false)
			if err != nil {
				fmt.Printf("Segment deserialization failed: %v\n", err)
				return
			}
			for i:=0; i<len(defragmentedRecords); i++ {
				fmt.Println(defragmentedRecords[i])
			}
		}
	}
}