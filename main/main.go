package main

import (
	"NASP-PROJEKAT/BlockCache"
	"NASP-PROJEKAT/BlockManager"
	"NASP-PROJEKAT/SSTable"
	"NASP-PROJEKAT/data"
	"NASP-PROJEKAT/memtable"
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
}

func main() {
	//DEFAULT VRIJEDNOSTI KONFIGURACIJE
	var BLOCK_SIZE uint64 = 70
	var MEMTABLE_SIZE uint64 = 30
	var MEMTABLE_COUNT uint64 = 2
	var MEMTABLE_TYPE string = "hashmap"
	//var CACHE_SIZE uint32 = 10
	var SUMMARY_SAMPLE uint64 = 5

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

	memtable, err := memtable.CreateMemtableManager(MEMTABLE_TYPE, int(MEMTABLE_COUNT), int(MEMTABLE_SIZE))
	if err != nil {
		panic(err)
	}

	dataSeg := &SSTable.DataSegment{}
	index := &SSTable.Index{}
	summary := &SSTable.Summary{
		Sample: SUMMARY_SAMPLE,
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

	files, _ := ioutil.ReadDir("../SSTable/files")
	numOfSSTables := len(files) / 3

	var input uint32
	var key string
	var value []byte

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
			//key = "key6"

			if record, _ := memtable.Get(key); record != nil {
				fmt.Println("Zapis je pronadjen : ", string(value))
				continue
			}
			/* if SEARCHMEMTABLE != nil
				continue

			else if SEARCHCACHE != nil
				continue

			*/
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
				sst.Summary.First = "key1"
				sst.Summary.Last = "key6"
				value = sst.Get(key)
				if value == nil {
					continue
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
			//PUT OPERACIJA

			fmt.Scan(&key, &value)

			/*
				writeToWAL

				writeToMEM

				if mem is full{

				flushedRecords, flush, err := memtable.Put(record)
				if err != nil{
					panic(err)
				} else if flush {
					// flushedRecords je niz pokazivaca za sstable
				}
			*/
			//	sst.MakeSSTable(records)

			numOfSSTables++
			sst := &SSTable.SSTable{
				DataSegment:     dataSeg,
				Index:           index,
				Summary:         summary,
				BlockManager:    blockManager,
				DataFilePath:    "../SSTable/files/data" + strconv.Itoa(numOfSSTables) + ".bin",
				IndexFilePath:   "../SSTable/files/index" + strconv.Itoa(numOfSSTables) + ".bin",
				SummaryFilePath: "../SSTable/files/summary" + strconv.Itoa(numOfSSTables) + ".bin",
			}
			records := []data.Record{
				{Crc: 12345, KeySize: 4, ValueSize: 5, Key: "key1", Value: []byte("val1"), Tombstone: false, Timestamp: "2024-06-14"},
				{Crc: 67890, KeySize: 4, ValueSize: 200, Key: "key2", Value: []byte("val2"), Tombstone: true, Timestamp: "2024-06-15"},
				{Crc: 54321, KeySize: 4, ValueSize: 140, Key: "key3", Value: []byte("val3"), Tombstone: false, Timestamp: "2024-06-16"},
				{Crc: 12345, KeySize: 4, ValueSize: 5, Key: "key4", Value: []byte("val4"), Tombstone: false, Timestamp: "2024-06-14"},
				{Crc: 67890, KeySize: 4, ValueSize: 20, Key: "key5", Value: []byte("val5"), Tombstone: true, Timestamp: "2024-06-15"},
				{Crc: 12345, KeySize: 4, ValueSize: 500, Key: "key6", Value: []byte("val6PERSA PERSIC"), Tombstone: false, Timestamp: "2024-06-14"},
			}

			var recordPtrs []*data.Record
			for i := range records {
				recordPtrs = append(recordPtrs, &records[i])
			}
			sst.MakeSSTable(recordPtrs)

			sst.Index = index
			sst.Summary = summary

			sst.WriteSSTable()

		} else if input == 3 {

			//DELETE OPERACIJA
			fmt.Scan(&key)

			/*
				writeToWAL

				updateMEM

				flushedRecords, flush, err := memtable.Delete(record)
				if err != nil{
					panic(err)
				} else if flush {
					// flushedRecords je niz pokazivaca za sstable
				}
			*/

		}
	}

}
