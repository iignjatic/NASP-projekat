package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
)

type countMinSketch struct {
	table         [][]uint
	hashFunctions []HashWithSeed
	k             uint
	m             uint
}

func createCountMinScetch(epsilon, delta float64) countMinSketch {
	k := CalculateK(delta)
	m := CalculateM(epsilon)
	hashFunctions := CreateHashFunctions(k)
	table := make([][]uint, k)
	for i := range table {
		table[i] = make([]uint, m)
	}

	return countMinSketch{
		table:         table,
		hashFunctions: hashFunctions,
		k:             k,
		m:             m,
	}

}

func addElement(countMinSketch *countMinSketch, key []byte) {
	hashValues := make([]uint64, countMinSketch.k)
	for i := range countMinSketch.hashFunctions {
		hashValues[i] = countMinSketch.hashFunctions[i].Hash(key)
		j := hashValues[i] % uint64(countMinSketch.m)
		countMinSketch.table[i][j]++
	}

}

func findFrequency(countMinSketch *countMinSketch, key []byte) uint {
	hashValues := make([]uint64, countMinSketch.k)
	valuesForKey := make([]uint, countMinSketch.k)
	for i := range countMinSketch.hashFunctions {
		hashValues[i] = countMinSketch.hashFunctions[i].Hash(key)
		j := hashValues[i] % uint64(countMinSketch.m)
		valuesForKey[i] = countMinSketch.table[i][j]

	}

	min := uint(999999)
	for i := range valuesForKey {
		if uint(valuesForKey[i]) < min {
			min = uint(valuesForKey[i])
		}
	}

	return uint(min)

}

func (cms *countMinSketch) serialize(filename string) {
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("Creating file error: %v", err)
	}
	defer file.Close()

	var bytes []byte
	kBytes := make([]byte, 4)
	mBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(kBytes, uint32(cms.k))
	binary.LittleEndian.PutUint32(mBytes, uint32(cms.m))
	bytes = append(bytes, kBytes...)
	bytes = append(bytes, mBytes...)

	for _, row := range cms.table {
		rowLength := make([]byte, 4)
		binary.LittleEndian.PutUint32(rowLength, uint32(len(row)))
		bytes = append(bytes, rowLength...)
		for _, value := range row {
			bytesValue := make([]byte, 4)
			binary.LittleEndian.PutUint32(bytesValue, uint32(value))
			bytes = append(bytes, bytesValue...)
		}

	}

	numberOfFunctions := make([]byte, 4)
	binary.LittleEndian.PutUint32(numberOfFunctions, uint32(len(cms.hashFunctions)))
	bytes = append(bytes, numberOfFunctions...)
	for _, hashFunction := range cms.hashFunctions {
		seedLength := make([]byte, 4)
		binary.LittleEndian.PutUint32(seedLength, uint32(len(hashFunction.Seed)))
		bytes = append(bytes, seedLength...)
		bytes = append(bytes, hashFunction.Seed...)
	}

	_, err = file.Write(bytes)
	if err != nil {
		log.Fatalf("Error in writing bytes in file: %v", err)
	}

}

func (cms *countMinSketch) deserialize(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Error in opening file: %v", err)
	}

	defer file.Close()

	var bytes []byte

	dataInFile, err := file.Stat()
	if err != nil {
		log.Fatalf("Error in reading data about file: %v", err)
	}

	fileSize := dataInFile.Size()
	bytes = make([]byte, fileSize)
	_, err = file.Read(bytes)
	if err != nil {
		log.Fatalf("Error in reading file: %v", err)
	}

	cms.k = uint(binary.LittleEndian.Uint32(bytes[:4]))
	cms.m = uint(binary.LittleEndian.Uint32(bytes[4:8]))
	bytes = bytes[8:]

	cms.table = make([][]uint, cms.k)
	for i := uint(0); i < cms.k; i++ {
		rowLength := binary.LittleEndian.Uint32(bytes[:4])
		bytes = bytes[4:]
		cms.table[i] = make([]uint, rowLength)
		for j := 0; j < int(rowLength); j++ {
			cms.table[i][j] = uint(binary.LittleEndian.Uint32(bytes[:4]))
			bytes = bytes[4:]
		}

	}

	numberOfFunctions := binary.LittleEndian.Uint32(bytes[:4])
	bytes = bytes[4:]
	cms.hashFunctions = make([]HashWithSeed, numberOfFunctions)

	for i := 0; i < int(numberOfFunctions); i++ {
		seedLength := binary.LittleEndian.Uint32(bytes[:4])
		bytes = bytes[4:]
		cms.hashFunctions[i].Seed = make([]byte, seedLength)
		copy(cms.hashFunctions[i].Seed, bytes[:seedLength])
		bytes = bytes[seedLength:]

	}

}

func (cms *countMinSketch) delete() {
	cms.table = nil
	cms.hashFunctions = nil
	cms.k = 0
	cms.m = 0
}

func main() {
	epsilon := 0.01
	delta := 0.01
	countMinSketch := createCountMinScetch(epsilon, delta)

	kljuc1 := []byte("jabuka")
	kljuc2 := []byte("banana")
	kljuc3 := []byte("narandza")
	kljuc4 := []byte("jabuka")

	addElement(&countMinSketch, kljuc1)
	addElement(&countMinSketch, kljuc2)
	addElement(&countMinSketch, kljuc3)
	addElement(&countMinSketch, kljuc4)

	fmt.Printf("Ucestalost jabuke je: %d\n", findFrequency(&countMinSketch, kljuc1))
	fmt.Printf("Ucestalost banane je: %d\n", findFrequency(&countMinSketch, kljuc2))
	fmt.Printf("Ucestalost narandze je: %d\n", findFrequency(&countMinSketch, kljuc3))

	imeFajla := "countMinSketch_podaci.bin"
	countMinSketch.serialize(imeFajla)
	fmt.Println("Uspijesno sam serijalizovao u fajl")

	countMinSketch2 := countMinSketch

	countMinSketch2.deserialize(imeFajla)
	fmt.Println("Uspijesno sam deserijalizovao iz fajla")

	fmt.Printf("Ucestalost jabuke (posle deserijalizacije) je: %d\n", findFrequency(&countMinSketch2, kljuc1))
	fmt.Printf("Ucestalost banane (posle deserijalizacije) je: %d\n", findFrequency(&countMinSketch2, kljuc2))
	fmt.Printf("Ucestalost narandze (posle deserijalizacije) je: %d\n", findFrequency(&countMinSketch2, kljuc3))

	countMinSketch.delete()
	if countMinSketch.hashFunctions == nil && countMinSketch.table == nil && countMinSketch.k == 0 && countMinSketch.m == 0 {
		fmt.Println("Uspijesno obrisan countMinSketch")
	}

}
