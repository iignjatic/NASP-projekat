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

type MemtableManager struct {
	tables    []*Memtable
	maxTables uint
	mutex     sync.Mutex
}

func CreateMemtable(maxSize uint, readOnly bool) *Memtable {
	return &Memtable{
		data:        make(map[string]*Record),
		maxSize:     maxSize,
		readOnly:    readOnly,
		currentSize: 0,
	}
}

func CreateMemtableManager(maxTables, tableSize uint) *MemtableManager {
	manager := &MemtableManager{
		tables:    make([]*Memtable, 0, maxTables),
		maxTables: maxTables,
	}

	// N-1 read-only tabela i 1 read-write tabela
	for i := 0; i < int(maxTables)-1; i++ {
		manager.tables = append(manager.tables, CreateMemtable(tableSize, true))
	}
	manager.tables = append(manager.tables, CreateMemtable(tableSize, false))
	return manager
}

// funkcija dodavanja record-a kod rada sa samo jednom mem tabelom
func (memt *Memtable) Put(record Record) error {
	memt.mutex.Lock()
	defer memt.mutex.Unlock()

	if memt.readOnly {
		return errors.New("cannot add to a read-only memtable")
	}

	if memt.currentSize >= memt.maxSize {
		return errors.New("memtable is full")
	}

	memt.data[record.Key] = &record
	memt.currentSize++
	return nil
}

func (mm *MemtableManager) Put(record Record) error {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	activeTable := mm.tables[len(mm.tables)-1]
	if err := activeTable.Put(record); err != nil {
		if err.Error() == "memtable is full" {
			if err := mm.Flush(); err != nil {
				return err
			}
			return activeTable.Put(record)
		}
		return err
	}
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

func (mm *MemtableManager) Flush() error {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	fmt.Println("Flushing Memtables...")
	for i := 0; i < len(mm.tables)-1; i++ {
		records, err := mm.tables[i].Flush()
		if err != nil {
			return err
		}
		fmt.Printf("Flushed table %d: %+v\n", i, records)
	}
	return nil
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
