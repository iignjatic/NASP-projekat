package main

import (
	"NASP-PROJEKAT/SSTable"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

type Config struct {
	BlockSize     int `json:"BlockSize"`
	MemTableSize  int `json:"MemTableSize"`
	CacheSize     int `json:"CacheSize"`
	SummarySample int `json:"SummarySample"`
}

func main() {
	//DEFAULT VRIJEDNOSTI KONFIGURACIJE
	var BLOCK_SIZE uint32 = 16
	var MEMTABLE_SIZE uint32 = 30
	var CACHE_SIZE uint32 = 10
	var SUMMARY_SAMPLE uint32 = 5

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
		BLOCK_SIZE = uint32(config.BlockSize)
		MEMTABLE_SIZE = uint32(config.MemTableSize)
		CACHE_SIZE = uint32(config.CacheSize)
		SUMMARY_SAMPLE = uint32(config.SummarySample)
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
		- sstable

	*/

	dataSeg := &SSTable.DataSegment{}
	index := &SSTable.Index{}
	summary := &SSTable.Summary{}
	blockManager := &SSTable.BlockManager{}

	// Kreiranje SSTable-a
	sst := &SSTable.SSTable{
		DataSegment:     dataSeg,
		Index:           index,
		Summary:         summary,
		BlockManager:    blockManager,
		DataFilePath:    "../SSTable/files/data.bin",
		IndexFilePath:   "../SSTable/files/index.bin",
		SummaryFilePath: "../SSTable/files/summary.bin",
	}

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

		if input == 1 {
			//GET operacija

			fmt.Scan(&key)

			/* if SEARCHMEMTABLE != nil
				continue

			else if SEARCHCACHE != nil
				continue

			else if SEARCHSSTABLE != nil
				UPDATE CACHE
				found key

			else
				not found


			*/
		} else if input == 2 {
			//PUT OPERACIJA

			fmt.Scan(&key, &value)

			/*
				writeToWAL

				writeToMEM

				if mem is full{
			*/
			//	sst.MakeSSTable(records)

			sst.Index = index
			sst.Summary = summary

			sst.WriteSSTable()
			//}

		} else if input == 3 {

			//DELETE OPERACIJA
			fmt.Scan(&key)

			/*
				writeToWAL

				updateMEM

			*/

		}
	}

}
