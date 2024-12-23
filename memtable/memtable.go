package main

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

type Record struct {
	Crc       uint32
	KeySize   uint64
	ValueSize uint64
	Key       string
	Value     []byte
	Tombstone bool
	Timestamp string
}

type Memtable struct {
	data        map[string]*Record
	maxSize     uint
	mutex       sync.RWMutex
	readOnly    bool
	currentSize uint
}

func CreateMemtable(maxSize uint, readOnly bool) *Memtable {
	return &Memtable{
		data:        make(map[string]*Record),
		maxSize:     maxSize,
		readOnly:    readOnly,
		currentSize: 0,
	}
}

func (memt *Memtable) Put(record Record) error {
	memt.mutex.Lock()
	defer memt.mutex.Unlock()

	if memt.readOnly {
		return errors.New("cannot add to a read-only memtable")
	}

	if memt.currentSize >= memt.maxSize {
		return errors.New("memtable is full, flush required")
	}

	memt.data[record.Key] = &record
	memt.currentSize++
	return nil
}

func (memt *Memtable) Get(key string) ([]byte, error) {
	memt.mutex.RLock()
	defer memt.mutex.RUnlock()

	record, exists := memt.data[key]
	if !exists || record.Tombstone {
		return nil, errors.New("key not found")
	}

	return record.Value, nil
}

// kada se popuni kapacitet upisuje u SSTable
func (memt *Memtable) Flush() ([]Record, error) {
	memt.mutex.Lock()
	defer memt.mutex.Unlock()

	if memt.currentSize == 0 {
		return nil, errors.New("nothing to flush")
	}

	records := make([]Record, 0, len(memt.data))
	for _, record := range memt.data {
		if record.Tombstone {
			continue
		}
		records = append(records, *record)
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].Key < records[j].Key
	})

	memt.data = make(map[string]*Record)
	memt.currentSize = 0
	return records, nil
}

// ukljanja zastarjele ili nepotrebne podatke iz prethodnih SSTable-ova
//func (memt *Memtable) compaction() {
//
//}

// brisanje elemenata
func (memt *Memtable) Delete(key string) error {
	memt.mutex.Lock()
	defer memt.mutex.Unlock()

	if memt.readOnly {
		return errors.New("cannot delete form a read-only memtable")
	}

	record, exists := memt.data[key]
	if !exists {
		return errors.New("key not found")
	}

	record.Tombstone = true
	record.Timestamp = time.Now().String()
	memt.data[key] = record
	return nil
}

func main() {
	memtable := CreateMemtable(5, false)

	record := Record{
		Key:       "example",
		Value:     []byte("value"),
		Timestamp: time.Now().String(),
	}

	err := memtable.Put(record)
	if err != nil {
		fmt.Println("Error adding record")
	}

	if records, err := memtable.Flush(); err != nil {
		fmt.Println("Error flushing", err)
	} else {
		fmt.Println("Flushed data: ", records)
	}
}
