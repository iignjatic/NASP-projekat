package memtable

import (
	"NASP-PROJEKAT/b_tree"
	"NASP-PROJEKAT/data"
	"errors"
	"fmt"
	"math"
)

type MemtableB struct {
	data        *b_tree.BTree
	maxSize     uint
	readOnly    bool
	currentSize uint
}

type MemtableManagerB struct {
	tables      []*MemtableB
	maxTables   uint
	oldestIndex uint
	acitveIndex uint
}

func CreateMemtableB(maxSize uint, readOnly bool) *MemtableB {
	return &MemtableB{
		data:        b_tree.NewBTree(int(math.Sqrt(float64(maxSize)))),
		maxSize:     maxSize,
		readOnly:    readOnly,
		currentSize: 0,
	}
}

// dodavanje Record strukture u Memtable
func (memt *MemtableB) AddRecord(record *data.Record) error {
	if memt.readOnly {
		return errors.New("cannot add to a read-only memtable")
	}

	memt.data.InsertRecord(record)
	memt.currentSize++
	return nil
}

// dobavljenje recorda prema kljucu iz jedne memtabele
func (memt *MemtableB) Get(key string) (*data.Record, error) {
	record, err := memt.data.Get(key)
	if err != nil || record.Tombstone {
		return nil, errors.New("key not found")
	}

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
		return err
	}

	record.Tombstone = true
	memt.data.InsertRecord(record)
	return nil
}

// flush sortira podatke po kljucu
// nakon upisivanja podataka na disk, oslobadja memtable
func (memt *MemtableB) Flush() ([]*data.Record, error) {
	if memt.currentSize == 0 {
		return nil, errors.New("nothing to flush")
	}

	records := memt.data.GetSortedRecords()

	// praznjenje memtable
	memt.data = b_tree.NewBTree(int(math.Sqrt(float64(memt.maxSize))))
	memt.currentSize = 0
	return records, nil
}

// kreiranje novog memtable menadzera koji ce raditi sa maxTables tabela, koji svaki imaju po maksimalno maxSize elementa
func CreateMemtableManagerBTree(maxTables, maxSize uint) *MemtableManagerB {
	manager := MemtableManagerB{
		tables:      make([]*MemtableB, 0, maxTables),
		maxTables:   maxTables,
		oldestIndex: 0,
		acitveIndex: 0,
	}

	memtable := CreateMemtableB(maxSize, false)
	manager.tables = append(manager.tables, memtable)

	for i := 0; i < int(maxTables)-1; i++ {
		memtable := CreateMemtableB(maxSize, true)
		manager.tables = append(manager.tables, memtable)
	}

	return &manager
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

// dodavanje novog recorda u odgovarajuci memtable
func (mm *MemtableManagerB) Put(record *data.Record) ([]*data.Record, bool, error) {
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
	// ako se nesto nalazi u flushedRecords znaci da se treba uraditi flush
	if flushedRecords != nil {
		return flushedRecords, true, nil
	}
	return flushedRecords, false, nil
}

// rotira memtabele, kada su sve popunjene "najstarija" tabela se flush-uje
// "najstarija" tabela se oslobadja i postaje nova aktivna tabela (read-write tabela)
// dok ona koja je bila aktivna postaje read-only
// ako sve tabele nisu popunjene, onda samo pomjera index akitvne tabele i azurira stanje read-only polja
func (mm *MemtableManagerB) RotateMemtables() ([]*data.Record, error) {
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

func (mm *MemtableManagerB) Get(key string) (*data.Record, error) {
	for i := 0; i < int(mm.maxTables); i++ {
		index := (int(mm.acitveIndex) - i + int(mm.maxTables)) % int(mm.maxTables)
		table := mm.tables[index]
		if record, err := table.data.Get(key); err == nil {
			if record.Tombstone {
				return nil, err
			}
			return record, nil
		}
	}
	return nil, errors.New("key not found")
}

func (mm *MemtableManagerB) Delete(record *data.Record) ([]*data.Record, bool, error) {
	acitveTable := mm.tables[mm.acitveIndex]

	existingRecord, err := acitveTable.data.Get(record.Key)
	if err != nil {
		flushedRecords, flush, err := mm.Put(record)
		if err != nil {
			return nil, false, err
		}
		return flushedRecords, flush, nil
	}
	existingRecord.Tombstone = true
	acitveTable.data.InsertRecord(existingRecord)
	return nil, false, nil
}

// vraca niz nizova pokazivaca data.Records,
// odnosno niz nizova podataka koji su flush-ovani, za koje treba da se kreira sstable
func (mm *MemtableManagerB) LoadFromWal(records []*data.Record) ([][]*data.Record, error) {
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
/*
func main() {
	// kreiraj menadzer sa 2 tabele kapaciteta 5
	memtableManager := CreateMemtableManagerBTree(2, 5)

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
