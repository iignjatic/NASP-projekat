package memtable

import (
	"NASP-PROJEKAT/data"
	"NASP-PROJEKAT/skiplist"
	"errors"
	"fmt"
)

type MemtableS struct {
	data        *skiplist.SkipList
	maxSize     uint
	readOnly    bool
	currentSize uint
}

type MemtableManagerS struct {
	tables      []*MemtableS
	maxTables   uint
	oldestIndex uint
	acitveIndex uint
}

// kreiranje nove Memtable
func CreateMemtableS(maxSize uint, readOnly bool) *MemtableS {
	return &MemtableS{
		data:        skiplist.NewSkipList(int(maxSize)),
		maxSize:     maxSize,
		readOnly:    readOnly,
		currentSize: 0,
	}
}

// dodavanje Record strukture u Memtable
func (memt *MemtableS) AddRecord(record *data.Record) error {
	if memt.readOnly {
		return errors.New("cannot add to a read-only memtable")
	}

	memt.data.AddElement(record.Key, record)
	memt.currentSize++
	return nil
}

// dobavljenje recorda prema kljucu iz jedne memtabele
func (memt *MemtableS) Get(key string) (*data.Record, error) {
	record := memt.data.SearchElement(key)
	if record == nil {
		return nil, errors.New("key not found")
	}

	return record, nil
}

func (memt *MemtableS) IsFull() bool {
	return memt.currentSize == memt.maxSize
}

// logicko brisanje recorda
func (memt *MemtableS) Delete(key string) error {
	if memt.readOnly {
		return errors.New("cannot delete from a read-only memtable")
	}

	record := memt.data.SearchElement(key)
	if record == nil {
		//fmt.Printf("Record za kljuc %s nije pronadjen, funkcija Delete()", key)
		return errors.New("key not found")
	}

	record.Tombstone = true
	memt.data.AddElement(record.Key, record)
	return nil
}

// flush sortira podatke po kljucu
// nakon upisivanja podataka na disk, oslobadja memtable
func (memt *MemtableS) Flush() ([]*data.Record, error) {
	fmt.Println("Radi se Flush()")
	if memt.currentSize == 0 {
		return nil, errors.New("nothing to flush")
	}

	records := memt.data.SortElements()

	// praznjenje memtable
	memt.data = skiplist.NewSkipList(int(memt.maxSize))
	memt.currentSize = 0
	return records, nil
}

// kreiranje novog memtable menadzera koji ce raditi sa maxTables tabela, koji svaki imaju po maksimalno maxSize elementa
func CreateMemtableManagerS(maxTables, maxSize uint) *MemtableManagerS {
	manager := MemtableManagerS{
		tables:      make([]*MemtableS, 0, maxTables),
		maxTables:   maxTables,
		oldestIndex: 0,
		acitveIndex: 0,
	}

	memtable := CreateMemtableS(maxSize, false)
	manager.tables = append(manager.tables, memtable)

	for i := 0; i < int(maxTables)-1; i++ {
		memtable := CreateMemtableS(maxSize, true)
		manager.tables = append(manager.tables, memtable)
	}

	return &manager
}

// provjerava da li su sve tabele popunjene
func (mm *MemtableManagerS) MemtableManagerIsFull() bool {
	for i := 0; i < int(mm.maxTables); i++ {
		if !mm.tables[i].IsFull() {
			return false
		}
	}
	return true
}

// dodavanje novog recorda u odgovarajuci memtable
func (mm *MemtableManagerS) Put(record *data.Record) ([]*data.Record, bool, error) {
	activeMemtable := mm.tables[mm.acitveIndex]

	if activeMemtable.readOnly {
		return nil, false, errors.New("cannot add to a read-only memtable")
	}

	var flushedRecords []*data.Record

	if activeMemtable.IsFull() {
		rec, err := mm.RotateMemtables()
		if err != nil {
			return nil, false, fmt.Errorf("failed to rotate memtables: %w", err)
		}
		flushedRecords = rec
		activeMemtable = mm.tables[mm.acitveIndex]
	}

	if err := activeMemtable.AddRecord(record); err != nil {
		return nil, false, err
	}

	if activeMemtable.currentSize == activeMemtable.maxSize && mm.MemtableManagerIsFull() {
		rec, err := mm.RotateMemtables()
		if err != nil {
			return nil, false, fmt.Errorf("failed to rotate memtables: %w", err)
		}
		flushedRecords = rec
	}

	return flushedRecords, true, nil
}

// rotira memtabele, kada su sve popunjene "najstarija" tabela se flush-uje
// "najstarija" tabela se oslobadja i postaje nova aktivna tabela (read-write tabela)
// dok ona koja je bila aktivna postaje read-only
// ako sve tabele nisu popunjene, onda samo pomjera index akitvne tabele i azurira stanje read-only polja
func (mm *MemtableManagerS) RotateMemtables() ([]*data.Record, error) {
	var records []*data.Record
	if mm.MemtableManagerIsFull() {
		oldestTable := mm.tables[mm.oldestIndex]
		rec, err := oldestTable.Flush()
		if err != nil {
			return nil, fmt.Errorf("failed to flush table at index %d: %w", mm.oldestIndex, err)
		}
		records = rec
		oldestTable.readOnly = false

		//mm.acitveIndex = mm.oldestIndex
		mm.oldestIndex = (mm.oldestIndex + 1) % mm.maxTables
	} else {
		mm.tables[mm.acitveIndex].readOnly = true
		mm.acitveIndex = (mm.acitveIndex + 1) % mm.maxTables
		mm.tables[mm.acitveIndex].readOnly = false
	}

	return records, nil
}

func (mm *MemtableManagerS) Get(key string) (*data.Record, error) {
	for i := 0; i < int(mm.maxTables); i++ {
		index := (int(mm.acitveIndex) - i + int(mm.maxTables)) % int(mm.maxTables)
		table := mm.tables[index]
		if record := table.data.SearchElement(key); record != nil {
			if record.Tombstone {
				return nil, errors.New("key not found")
			}
			return record, nil
		}
	}
	return nil, errors.New("key not found")
}

func (mm *MemtableManagerS) Delete(record *data.Record) ([]*data.Record, bool, error) {
	acitveTable := mm.tables[mm.acitveIndex]

	found_record := acitveTable.data.SearchElement(record.Key)
	if found_record == nil {
		flushedRecords, flush, err := mm.Put(record)
		if err != nil {
			return nil, false, err
		}
		return flushedRecords, flush, nil
	}
	record.Tombstone = true
	acitveTable.data.AddElement(record.Key, record)
	return nil, false, nil
}

func (mm *MemtableManagerS) LoadFromWal(records []*data.Record) ([][]*data.Record, error) {
	var recordsToFlush [][]*data.Record
	for _, rec := range records {
		records, flush, err := mm.Put(rec)
		if err != nil {
			return nil, err
		} else if flush {
			recordsToFlush = append(recordsToFlush, records)
		}
	}
	return recordsToFlush, nil
}

/*
func main() {
	// kreiraj menadzer sa 2 tabele kapaciteta 5
	memtableManager := CreateMemtableManagerS(2, 5)

	// dodaj rekorde dok se sve tabele ne popune
	for i := 1; i <= 10; i++ {
		_, _, err := memtableManager.Put(&data.Record{
			Key:       fmt.Sprintf("key%d", i),
			Value:     []byte(fmt.Sprintf("value%d", i)),
			Tombstone: false,
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
		if err != nil {
			fmt.Printf("Greška pri dodavanju recorda key%d: %v\n", i, err)
		} else {
			fmt.Printf("Record key%d uspešno dodat u tabelu %d!\n", i, memtableManager.acitveIndex)
		}

		// Nakon dodavanja 5 rekorda očekujemo automatsku rotaciju
		if i == 5 {
			fmt.Println("Tabla popunenja, ocekluje se rotacija")
		}

		if i == 10 {
			fmt.Println("Sve tabele popunjenje, ocekuje se automatski flush")
		}
	}

	// Testiraj dobijanje recorda nakon flushovanja
	for i := 1; i <= 10; i++ {
		record, err := memtableManager.Get(fmt.Sprintf("key%d", i))
		if err != nil {
			fmt.Printf("Greška pri dobijanju recorda key%d: %v\n", i, err)
		} else {
			fmt.Printf("Record sa ključem 'key%d': %v\n", i, record)
		}
	}

	// Testiraj brisanje i ponovno dobijanje recorda
	err := memtableManager.Delete("key3")
	if err != nil {
		fmt.Println("Greška pri brisanju recorda key3:", err)
	} else {
		fmt.Println("Record sa ključem 'key3' obrisan!")
	}

	record, err := memtableManager.Get("key3")
	if err != nil {
		fmt.Println("Greška pri dobijanju recorda key3 (trebalo bi da ne postoji):", err)
	} else {
		fmt.Printf("Record sa ključem 'key3' (trebalo bi da ne postoji): %v\n", record)
	}

	// Testiraj dodavanje novih rekorda nakon flushovanja
	for i := 11; i <= 12; i++ {
		_, _, err := memtableManager.Put(&data.Record{
			Key:       fmt.Sprintf("value%d", i),
			Value:     []byte(fmt.Sprintf("value%d", i)),
			Tombstone: false,
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
		if err != nil {
			fmt.Printf("Greška pri dodavanju recorda key%d: %v\n", i, err)
		} else {
			fmt.Printf("Record key%d uspešno dodat u tabelu %d!\n", i, memtableManager.acitveIndex)
		}
	}

}*/
