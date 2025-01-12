package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"time"
)

const BlockSize = 1024

type Record2 struct {
	Key       string
	Value     []byte
	Tombstone bool
	Timestamp string
}

type Memtable2 struct {
	data        map[string]*Record2
	maxSize     uint
	readOnly    bool
	currentSize uint
}

type MemtableManager2 struct {
	tables      []*Memtable2
	maxTables   uint
	oldestIndex uint
	acitveIndex uint
}

// strukutra koja se koristi u wal-u samo privremeno se ovdje koristi dok wal ne bude na develop grani
type Wal struct {
	Segments       []*Segment // Array of Segments
	Directory      string     // Directory where Segments are stored
	CurrentSegment *Segment   // Current active segment
	SegmentSize    int        // Maximum number of blocks per Segment
	NextSegmentID  int        // ID of the next Segment
}

type Segment struct {
	ID              int      // Segment ID
	Blocks          []*Block // Array of Blocks
	CurrentCapacity int      // Current number of blocks in a Segment
	FullCapacity    int      // Max number of blocks in a Segment
	FilePath        string   // The path to the file where the segment is stored
}

type Block struct {
	ID              int
	Records         []byte
	FullCapacity    uint32
	CurrentCapacity uint32
}

// kreiranje nove Memtable
func CreateMemtable2(maxSize uint, readOnly bool) *Memtable2 {
	return &Memtable2{
		data:        make(map[string]*Record2),
		maxSize:     maxSize,
		readOnly:    readOnly,
		currentSize: 0,
	}
}

// dodavanje Record strukture u Memtable
func (memt *Memtable2) AddRecord(record Record2) error {
	if memt.readOnly {
		return errors.New("cannot add to a read-only memtable")
	}

	/*if memt.currentSize >= memt.maxSize {
		_, err := memt.Flush()
		if err != nil {
			return err
		}
	}*/

	memt.data[record.Key] = &record
	memt.currentSize++
	return nil
}

// dodavanje novog para kljuc-vrijednost u memtable
func (memt *Memtable2) AddNewRecord(key string, value []byte) error {
	if memt.readOnly {
		return errors.New("cannot add to a read-only memtable")
	}

	if memt.currentSize >= memt.maxSize {
		_, err := memt.Flush()
		if err != nil {
			return err
		}
	}

	record := Record2{Key: key, Value: value, Tombstone: false, Timestamp: time.Now().UTC().Format(time.RFC3339)}
	memt.data[key] = &record
	memt.currentSize++
	return nil
}

// dobavljenje recorda prema kljucu
func (memt *Memtable2) Get(key string) (*Record2, error) {
	record, exist := memt.data[key]
	if !exist || record.Tombstone {
		return nil, errors.New("key not found")
	}

	return record, nil
}

// logicko brisanje recorda
func (memt Memtable2) Delete(key string) error {
	if memt.readOnly {
		return errors.New("cannot delete from a read-only memtable")
	}

	record, exist := memt.data[key]
	if !exist {
		return errors.New("key not found")
	}

	record.Tombstone = true
	record.Timestamp = time.Now().UTC().Format(time.RFC3339)
	memt.data[key] = record
	return nil
}

// flush sortira podatke po kljucu, eliminise one koji su logicki obrisani
// nakon upisivanja podataka na disk, oslobadja memtable
func (memt *Memtable2) Flush() ([]Record2, error) {
	if memt.currentSize == 0 {
		return nil, errors.New("nothing to flush")
	}

	records := make([]Record2, 0, len(memt.data))
	for _, record := range memt.data {
		/*if record.Tombstone {
			continue
		}*/
		records = append(records, *record)
	}

	// sortiranje
	sort.Slice(records, func(i, j int) bool {
		return records[i].Key < records[j].Key
	})

	// flushing data
	// SSTable logic

	// praznjenje memtable
	memt.data = make(map[string]*Record2)
	memt.currentSize = 0
	return records, nil
}

// kreiranje novog memtable menadzera koji ce raditi sa maxTables tabela, koji svaki imaju po maksimalno maxSize elementa
func CreateMemtableManager2(maxTables, maxSize uint) *MemtableManager2 {
	manager := MemtableManager2{
		tables:      make([]*Memtable2, 0, maxTables),
		maxTables:   maxTables,
		oldestIndex: 0,
		acitveIndex: maxTables - 1,
	}

	for i := 0; i < int(maxTables)-1; i++ {
		memtable := CreateMemtable2(maxSize, true)
		manager.tables = append(manager.tables, memtable)
	}
	memtable := CreateMemtable2(maxSize, false)
	manager.tables = append(manager.tables, memtable)
	return &manager
}

func (mm *MemtableManager2) AddRecord(record Record2) error {
	activeMemtable := mm.tables[mm.acitveIndex]

	if activeMemtable.readOnly {
		return errors.New("cannot add to a read-only memtable")
	}

	if activeMemtable.currentSize >= activeMemtable.maxSize {
		if err := mm.RotateMemtables(); err != nil {
			return fmt.Errorf("failed to rotate memtables: %w", err)
		}
	}

	return activeMemtable.AddRecord(record)
}

func (mm *MemtableManager2) RotateMemtables() error {
	if mm.acitveIndex == mm.oldestIndex {
		oldestTable := mm.tables[mm.oldestIndex]
		if _, err := oldestTable.Flush(); err != nil {
			return fmt.Errorf("failed to flush table at index %d: %w", mm.oldestIndex, err)
		}
		oldestTable.readOnly = false

		mm.acitveIndex = mm.oldestIndex
		mm.oldestIndex = (mm.oldestIndex + 1) % mm.maxTables
	} else {
		mm.tables[mm.acitveIndex].readOnly = true
		mm.acitveIndex = (mm.acitveIndex + 1) % mm.maxTables
		mm.tables[mm.acitveIndex].readOnly = false
	}

	return nil
}

func (mm *MemtableManager2) GetRecord(key string) (*Record2, error) {
	for i := 0; i < int(mm.maxTables); i++ {
		index := (int(mm.acitveIndex) - i + int(mm.maxTables)) % int(mm.maxTables)
		table := mm.tables[index]
		if record, exists := table.data[key]; exists {
			return record, nil
		}
	}
	return nil, errors.New("key not found")
}

func (mm *MemtableManager2) DeleteRecord(key string) error {
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
func (mm *MemtableManager2) FlushAll() error {
	for i := 0; i < int(mm.maxTables); i++ {
		table := mm.tables[i]
		if _, err := table.Flush(); err != nil {
			return fmt.Errorf("failed to flush table at index %d: %w", i, err)
		}
	}
	return nil
}

// funkcija iz record.go u wal strukturi koja jos nije dostupa na develop grani
func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// funkcija iz record.go u wal strukturi koja jos nije dostupna na develop grani
func FromBytes(data []byte) (*Record, error) {
	offset := 0

	crc := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	if offset+8 > len(data) {
		return nil, fmt.Errorf("insufficient data for keySize")
	}
	keySize := binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	if offset+8 > len(data) {
		return nil, fmt.Errorf("insufficient data for valueSize")
	}
	valueSize := binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	if offset+int(keySize) > len(data) {
		return nil, fmt.Errorf("insufficient data for key")
	}
	key := string(data[offset : offset+int(keySize)])
	offset += int(keySize)

	if offset+int(valueSize) > len(data) {
		return nil, fmt.Errorf("insufficient data for value")
	}
	value := data[offset : offset+int(valueSize)]
	offset += int(valueSize)

	if offset+1 > len(data) {
		return nil, fmt.Errorf("insufficient data for tombstone")
	}
	tombstone := data[offset] == 1
	offset++

	if offset > len(data) {
		return nil, fmt.Errorf("insufficient data for timestamp")
	}
	timestamp := string(data[offset:])

	calculatedCrc := CRC32(data[4:])
	if crc != calculatedCrc {
		return nil, fmt.Errorf("crc mismatch: expected %d, got %d", crc, calculatedCrc)
	}

	return &Record{
		Crc:       crc,
		KeySize:   keySize,
		ValueSize: valueSize,
		Key:       key,
		Value:     value,
		Tombstone: tombstone,
		Timestamp: timestamp,
	}, nil
}

func (mm *MemtableManager2) LoadFromWal(wal *Wal) error {
	for _, segment := range wal.Segments {
		for _, block := range segment.Blocks {
			offset := 0
			for offset < len(block.Records) {
				record, err := FromBytes(block.Records[offset:])
				if err != nil {
					return fmt.Errorf("failed to parse record from block %d in segmet %d: %w", block.ID, segment.ID, err)
				}

				err = mm.AddRecord(Record2{
					Key:       record.Key,
					Value:     record.Value,
					Tombstone: record.Tombstone,
					Timestamp: record.Timestamp,
				})
				if err != nil {
					return fmt.Errorf("failed to add record to memtable: %w", err)
				}

				recordSize := 4 + 8 + 1 + 8 + 8 + record.KeySize + record.ValueSize
				offset += int(recordSize)
			}
		}
	}
	return nil
}

func testing() {
	// Kreiraj menadžer sa 2 tabele
	memtableManager := CreateMemtableManager2(2, 5)

	// Dodaj record u aktivnu tabelu
	err := memtableManager.AddRecord(Record2{
		Key:       "key1",
		Value:     []byte("value1"),
		Tombstone: false,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		fmt.Println("Greška pri dodavanju recorda:", err)
	} else {
		fmt.Println("Record1 uspešno dodat!")
	}

	// Dodaj još jedan record
	err = memtableManager.AddRecord(Record2{
		Key:       "key2",
		Value:     []byte("value2"),
		Tombstone: false,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		fmt.Println("Greška pri dodavanju recorda:", err)
	} else {
		fmt.Println("Record2 uspešno dodat!")
	}

	// Testiraj dobijanje recorda
	record, err := memtableManager.GetRecord("key1")
	if err != nil {
		fmt.Println("Greška pri dobijanju recorda:", err)
	} else {
		fmt.Printf("Record sa ključem 'key1': %v\n", record)
	}

	// Testiraj brisanje recorda
	err = memtableManager.DeleteRecord("key1")
	if err != nil {
		fmt.Println("Greška pri brisanju recorda:", err)
	} else {
		fmt.Println("Record sa ključem 'key1' obrisan!")
	}

	// Pokušaj da se ponovo dođe do obrisanog recorda
	record, err = memtableManager.GetRecord("key1")
	if err != nil {
		fmt.Println("Greška pri dobijanju recorda:", err)
	} else {
		fmt.Printf("Record sa ključem 'key1' (trebalo bi da ne postoji): %v\n", record)
	}

	// Rotate Memtables
	err = memtableManager.RotateMemtables()
	if err != nil {
		fmt.Println("Greška pri rotiranju memtabela:", err)
	} else {
		fmt.Println("Memtable rotiran!")
	}

	// Flush svih tabela
	err = memtableManager.FlushAll()
	if err != nil {
		fmt.Println("Greška pri flushovanju svih tabela:", err)
	} else {
		fmt.Println("Sve tabele su uspešno flushovane!")
	}
}
