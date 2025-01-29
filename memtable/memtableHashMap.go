package memtable

import (
	"NASP-PROJEKAT/data"
	"errors"
	"fmt"
	"sort"
	"time"
)

type Memtable struct {
	data        map[string]*data.Record
	maxSize     uint
	readOnly    bool
	currentSize uint
}

type MemtableManager struct {
	tables      []*Memtable
	maxTables   uint
	oldestIndex uint
	acitveIndex uint
}

// kreiranje nove Memtable
func CreateMemtable(maxSize uint, readOnly bool) *Memtable {
	return &Memtable{
		data:        make(map[string]*data.Record),
		maxSize:     maxSize,
		readOnly:    readOnly,
		currentSize: 0,
	}
}

// dodavanje Record strukture u Memtable
func (memt *Memtable) AddRecord(record data.Record) error {
	if memt.readOnly {
		return errors.New("cannot add to a read-only memtable")
	}

	//fmt.Printf("Dodavanje recorda sa kljucem %s\n", record.Key)

	memt.data[record.Key] = &record
	memt.currentSize++
	return nil
}

// dobavljenje recorda prema kljucu iz jedne memtabele
func (memt *Memtable) Get(key string) (*data.Record, error) {
	record, exist := memt.data[key]
	if !exist || record.Tombstone {
		return nil, errors.New("key not found")
	}

	//fmt.Printf("Pronadjen record sa kljucem %s\n", key)
	return record, nil
}

func (memt *Memtable) IsFull() bool {
	return memt.currentSize == memt.maxSize
}

// logicko brisanje recorda
func (memt *Memtable) Delete(key string) error {
	if memt.readOnly {
		return errors.New("cannot delete from a read-only memtable")
	}

	record, exist := memt.data[key]
	if !exist {
		//fmt.Printf("Record za kljuc %s nije pronadjen, funkcija Delete()", key)
		return errors.New("key not found")
	}

	record.Tombstone = true
	record.Timestamp = time.Now().UTC().Format(time.RFC3339)
	memt.data[key] = record
	return nil
}

// flush sortira podatke po kljucu
// nakon upisivanja podataka na disk, oslobadja memtable
func (memt *Memtable) Flush() ([]*data.Record, error) {
	fmt.Println("Radi se Flush()")
	if memt.currentSize == 0 {
		return nil, errors.New("nothing to flush")
	}

	records := make([]*data.Record, 0, len(memt.data))
	for _, record := range memt.data {
		records = append(records, record)
	}

	// sortiranje
	sort.Slice(records, func(i, j int) bool {
		return records[i].Key < records[j].Key
	})

	// flushing data
	// SSTable logic

	//fmt.Println("Flush() zapisani podaci na disku")

	// praznjenje memtable
	memt.data = make(map[string]*data.Record)
	memt.currentSize = 0
	return records, nil
}

// kreiranje novog memtable menadzera koji ce raditi sa maxTables tabela, koji svaki imaju po maksimalno maxSize elementa
func CreateMemtableManager(maxTables, maxSize uint) *MemtableManager {
	manager := MemtableManager{
		tables:      make([]*Memtable, 0, maxTables),
		maxTables:   maxTables,
		oldestIndex: 0,
		acitveIndex: 0,
	}

	memtable := CreateMemtable(maxSize, false)
	manager.tables = append(manager.tables, memtable)

	for i := 0; i < int(maxTables)-1; i++ {
		memtable := CreateMemtable(maxSize, true)
		manager.tables = append(manager.tables, memtable)
	}

	return &manager
}

// provjerava da li su sve tabele popunjene
func (mm *MemtableManager) MemtableManagerIsFull() bool {
	for i := 0; i < int(mm.maxTables); i++ {
		if !mm.tables[i].IsFull() {
			return false
		}
	}
	return true
}

// dodavanje novog recorda u odgovarajuci memtable
func (mm *MemtableManager) AddRecord(record data.Record) error {
	activeMemtable := mm.tables[mm.acitveIndex]

	if activeMemtable.readOnly {
		return errors.New("cannot add to a read-only memtable")
	}

	if activeMemtable.IsFull() {
		//fmt.Println("Aktivna memtable je puna, rotiranje tabela")
		if err := mm.RotateMemtables(); err != nil {
			return fmt.Errorf("failed to rotate memtables: %w", err)
		}
		activeMemtable = mm.tables[mm.acitveIndex]
	}

	if err := activeMemtable.AddRecord(record); err != nil {
		return err
	}

	if activeMemtable.currentSize == activeMemtable.maxSize && mm.MemtableManagerIsFull() {
		if err := mm.RotateMemtables(); err != nil {
			return fmt.Errorf("failed to rotate memtables: %w", err)
		}
	}

	return nil
}

// rotira memtabele, kada su sve popunjene "najstarija" tabela se flush-uje
// "najstarija" tabela se oslobadja i postaje nova aktivna tabela (read-write tabela)
// dok ona koja je bila aktivna postaje read-only
// ako sve tabele nisu popunjene, onda samo pomjera index akitvne tabele i azurira stanje read-only polja
func (mm *MemtableManager) RotateMemtables() error {
	//fmt.Println("Radi se RotateMemtables()")
	if mm.MemtableManagerIsFull() {
		oldestTable := mm.tables[mm.oldestIndex]
		if _, err := oldestTable.Flush(); err != nil {
			return fmt.Errorf("failed to flush table at index %d: %w", mm.oldestIndex, err)
		}
		//fmt.Printf("Flush() tabele indeksa %d", mm.oldestIndex)
		oldestTable.readOnly = false

		//mm.acitveIndex = mm.oldestIndex
		mm.oldestIndex = (mm.oldestIndex + 1) % mm.maxTables
	} else {
		mm.tables[mm.acitveIndex].readOnly = true
		mm.acitveIndex = (mm.acitveIndex + 1) % mm.maxTables
		mm.tables[mm.acitveIndex].readOnly = false
	}

	return nil
}

func (mm *MemtableManager) GetRecord(key string) (*data.Record, error) {
	for i := 0; i < int(mm.maxTables); i++ {
		index := (int(mm.acitveIndex) - i + int(mm.maxTables)) % int(mm.maxTables)
		table := mm.tables[index]
		if record, exists := table.data[key]; exists {
			if record.Tombstone {
				return nil, errors.New("key not found")
			}
			return record, nil
		}
	}
	return nil, errors.New("key not found")
}

func (mm *MemtableManager) DeleteRecord(key string) error {
	acitveTable := mm.tables[mm.acitveIndex]
	if acitveTable.readOnly {
		return errors.New("cannot delete form read-only table")
	}

	record, exist := acitveTable.data[key]
	if !exist {
		return errors.New("key not found")
	}
	record.Tombstone = true
	record.Timestamp = time.Now().UTC().Format(time.RFC3339)
	acitveTable.data[key] = record
	return nil
}

// flush svih tabela, npr ako je potrebno prije iskljucenja sistema
func (mm *MemtableManager) FlushAll() error {
	fmt.Println("Radi se FlushAll()")
	for i := 0; i < int(mm.maxTables); i++ {
		table := mm.tables[i]
		if _, err := table.Flush(); err != nil {
			return fmt.Errorf("failed to flush table at index %d: %w", i, err)
		}
	}
	return nil
}
func (mm *MemtableManager) LoadFromWal(records []data.Record) {
	for _, rec := range records {
		mm.AddRecord(rec)
	}
}

/*
func main() {
	// kreiraj menadzer sa 2 tabele kapaciteta 5
	memtableManager := CreateMemtableManager(2, 5)

	// dodaj rekorde dok se sve tabele ne popune
	for i := 1; i <= 10; i++ {
		err := memtableManager.AddRecord(data.Record{
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
		record, err := memtableManager.GetRecord(fmt.Sprintf("key%d", i))
		if err != nil {
			fmt.Printf("Greška pri dobijanju recorda key%d: %v\n", i, err)
		} else {
			fmt.Printf("Record sa ključem 'key%d': %v\n", i, record)
		}
	}

	// Testiraj brisanje i ponovno dobijanje recorda
	err := memtableManager.DeleteRecord("key3")
	if err != nil {
		fmt.Println("Greška pri brisanju recorda key3:", err)
	} else {
		fmt.Println("Record sa ključem 'key3' obrisan!")
	}

	record, err := memtableManager.GetRecord("key3")
	if err != nil {
		fmt.Println("Greška pri dobijanju recorda key3 (trebalo bi da ne postoji):", err)
	} else {
		fmt.Printf("Record sa ključem 'key3' (trebalo bi da ne postoji): %v\n", record)
	}

	// Testiraj dodavanje novih rekorda nakon flushovanja
	for i := 11; i <= 12; i++ {
		err := memtableManager.AddRecord(data.Record{
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

	// Završni flush svih tabela
	err = memtableManager.FlushAll()
	if err != nil {
		fmt.Println("Greška pri flushovanju svih tabela:", err)
	} else {
		fmt.Println("Sve tabele su uspešno flushovane!")
	}
}*/
