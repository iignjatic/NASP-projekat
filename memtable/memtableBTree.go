package memtable

import (
	"NASP-PROJEKAT/b_tree"
	"NASP-PROJEKAT/data"
	"errors"
	"fmt"
	"time"
)

const ORDER = 4

type MemtableB struct {
	data        b_tree.BTree
	maxSize     uint
	readOnly    bool
	currentSize uint
}

type MemtableManagerB struct {
	tables      []*Memtable
	maxTables   uint
	oldestIndex uint
	acitveIndex uint
}

func CreateMemtableB(maxSize uint, readOnly bool) *MemtableB {
	return &MemtableB{
		data:        *b_tree.NewBTree(ORDER),
		maxSize:     maxSize,
		readOnly:    readOnly,
		currentSize: 0,
	}
}

// dodavanje Record strukture u Memtable
func (memt *MemtableB) AddRecord(record data.Record) error {
	if memt.readOnly {
		return errors.New("cannot add to a read-only memtable")
	}

	fmt.Printf("Dodavanje recorda sa kljucem %s\n", record.Key)

	memt.data.InsertRecord(&record)
	memt.currentSize++
	return nil
}

// dodavanje novog para kljuc-vrijednost u memtable
/*func (memt *MemtableB) AddNewRecord(key string, value []byte) error {
	if memt.readOnly {
		return errors.New("cannot add to a read-only memtable")
	}

	if memt.currentSize >= memt.maxSize {
		_, err := memt.Flush()
		if err != nil {
			return err
		}
	}

	record := data.Record{Key: key, Value: value, Tombstone: false, Timestamp: time.Now().UTC().Format(time.RFC3339), KeySize: uint64(len(key)), ValueSize: uint64(len(value))}
	memt.data.InsertRecord(&record)
	memt.currentSize++
	return nil
}*/

// dobavljenje recorda prema kljucu iz jedne memtabele
func (memt *MemtableB) Get(key string) (*data.Record, error) {
	record, err := memt.data.Get(key)
	if err != nil || record.Tombstone {
		return nil, errors.New("key not found")
	}

	//fmt.Printf("Pronadjen record sa kljucem %s\n", key)
	return record, nil
}

func (memt *MemtableB) IsFull() bool {
	return memt.currentSize == memt.maxSize
}

// logicko brisanje recorda
func (memt *MemtableB) Delete(key string) error {
	if memt.readOnly {
		return errors.New("cannot delete from a read-only memtable")
	}

	record, err := memt.data.Get(key)
	if err != nil {
		//fmt.Printf("Record za kljuc %s nije pronadjen, funkcija Delete()", key)
		return err
	}

	record.Tombstone = true
	record.Timestamp = time.Now().UTC().Format(time.RFC3339)
	memt.data.InsertRecord(record)
	return nil
}

// provjerava da li su sve tabele popunjene
func (mm *MemtableManagerB) MemtableManagerIsFull() bool {
	for i := 0; i < int(mm.maxTables); i++ {
		if !mm.tables[i].IsFull() {
			return false
		}
	}
	return true
}
