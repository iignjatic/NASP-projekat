package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const DIRECTORY_PATH = "segments"

type Wal struct {
	DirectoryPath  string     // directory where Segments are stored
	Segments       []*Segment // array of Segments that are not saved
	CurrentSegment *Segment   // current active segment
	SegmentPaths   []string   // list of all segment paths
}

func NewWal() *Wal {
	w := &Wal{
		DirectoryPath:  DIRECTORY_PATH,
		Segments:       []*Segment{},
		CurrentSegment: nil,
		SegmentPaths:   []string{},
	}
	w.AddNewSegment()
	return w
}

func (w *Wal) AddNewSegment() {
	w.SegmentPaths = w.ReadSegmentNames()

	// get the last segment
	segmentNames := w.ReadSegmentNames()
	var lastSegment string
	if len(segmentNames) == 0 {
		newSegmentID := len(w.SegmentPaths)
		newSegment := NewSegment(newSegmentID)
		w.Segments = append(w.Segments, newSegment)
		w.CurrentSegment = newSegment
		w.SegmentPaths = append(w.SegmentPaths, newSegment.FilePath)
		fmt.Printf("Created new Segment %s\n", newSegment.FilePath)
		return
	}
	lastSegment = segmentNames[len(segmentNames)-1]

	// check if the last segment is not full by reading the first byte
	ifFull, err := w.ReadFirstByte(lastSegment)
	if err != nil && ifFull != 0 && ifFull != 1 {
		err := errors.New("cannot read the first byte")
		fmt.Println("Error: ", err) 
		return
	}

	if ifFull == 0 {
		// if the last segment is not full, make it current segment
		segmentPath := filepath.Join(w.DirectoryPath, lastSegment)
		records, err := w.ReadSegmentFromFile(segmentPath)
		if err != nil {
			fmt.Printf("Error reading records from segment %s: %v\n", segmentPath, err)
			return
		}
		noZerosRecords := NoZerosRecords(records)
		defragmentedRecords := DefragmentRecords(noZerosRecords)

		// create the segment and add records to it
		lastUsedSegment := NewSegment(len(w.SegmentPaths)-1)
		w.Segments = append(w.Segments, lastUsedSegment)
		w.CurrentSegment = lastUsedSegment
		for _, record := range defragmentedRecords {
			rec := NewRecord(record.Key, append([]byte{}, record.Value...))
			if record.Type == 'f' {
				rec.Type = 'f'
			} else if record.Type == 'm' {
				rec.Type = 'm'
			} else if record.Type == 'l' {
				rec.Type = 'l'
			}
			rec.Timestamp = record.Timestamp
			rec.Crc = record.Crc
			w.AddRecord(rec)
		}
		// w.CurrentSegment.PrintBlocks()
	} else {
		newSegmentID := len(w.SegmentPaths)
		newSegment := NewSegment(newSegmentID)
		w.Segments = append(w.Segments, newSegment)
		w.CurrentSegment = newSegment
		w.SegmentPaths = append(w.SegmentPaths, newSegment.FilePath)
		fmt.Printf("Created new Segment %s\n", newSegment.FilePath)
	}
}

func (w *Wal) ReadSegmentNames() []string {
	files, err := os.ReadDir(w.DirectoryPath)
	if err != nil {
		fmt.Printf("Error reading directory: %v", err)
		return nil
	}
	var segmentNames []string
	for i:=0; i<len(files); i++ {
		if !files[i].IsDir() {
			segmentNames = append(segmentNames, files[i].Name())
		}
	}
	return segmentNames
}

func (w *Wal) AddRecord(record *Record) {
	w.AddRecordToBlock(record)
}

func (w *Wal) FlushCurrentSegment() {
	if w.CurrentSegment != nil {
		w.WriteSegmentToFile(w.CurrentSegment)
		w.CurrentSegment.Transferred = true
	}
}

// a function that writes records to the segment file
func (w *Wal) WriteSegmentToFile(s *Segment) error {
	if err := os.MkdirAll(w.DirectoryPath, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}
	filePath := w.DirectoryPath + "/" + s.FilePath
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// reserve one byte for indicator of fullness of the segment and eight bytes for number of bytes that are pushed on memtable from this segment IF THE SEGMENT IS FULL
	offset := make([]byte, 9)
	binary.LittleEndian.PutUint64(offset, 0)
	_, err = file.Write(offset)
	if err != nil {
		return err
	}

	for i:=0;i<len(s.Blocks);i++ {
		for j:=0;j<len(s.Blocks[i].Records);j++ {
			recordBytes, err := s.Blocks[i].Records[j].ToBytes()
			if err != nil {
				return err
			}
			_, err = file.Write(recordBytes)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// just change the fist byte to one if the segment is full
func (w *Wal) HandleFullness(s *Segment) error {
	file, err := os.OpenFile(w.DirectoryPath + "/" + s.FilePath, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek to beginning of the file: %w", err)
	}
	offset := []byte{1}
	_, err = file.Write(offset)
	if err != nil {
		return err
	}
	return nil
}

// a function that reads the whole segment record by record, or from a particular position in the file
func (w *Wal) ReadSegmentFromFile(filePath string) ([]*Record, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	// move the pointer to the second byte - first byte is for fullness of the file
	_, err = file.Seek(1, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to first byte: %w", err)
	}
	
	// read offset
	offsetBytes := make([]byte, 8)
	_, err = file.Read(offsetBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to read offset: %w", err)
	}

	offset := binary.LittleEndian.Uint64(offsetBytes) // convert offset to uint64

	// set the position to offset
	if offset != 0 {
		_, err := file.Seek(int64(offset), io.SeekCurrent)
		if err != nil {
			return nil, fmt.Errorf("failed to seek to offset %d: %w", offset, err)
		}
	}

	var records []*Record
	buffer := make([]byte, BLOCK_SIZE)
	var data []byte

	for {
		n, err := file.Read(buffer)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read file: %w", err)
		}
		if n == 0 {
			break
		}
		
		// handles leftovers
		data = append(data, buffer[:n]...)

		i:=0
		for i<len(data) {
			record, err := FromBytes(data[i:])
			if err != nil {
				return nil, fmt.Errorf("failed to parse record at position %d: %w",i, err)
			}
			records = append(records, record)
			recordBytes, err := record.ToBytes()
			if err != nil {
				return nil, fmt.Errorf("failed to serialize record to bytes: %w", err)
			}
			i += len(recordBytes)
		}
		data = data[i:]
	}
	return records, nil
}

func (w *Wal) ReadFirstByte(segmentPath string) (byte, error) {
	file, err := os.OpenFile(w.DirectoryPath + "/" + segmentPath, os.O_RDONLY, 0644)
	if err != nil {
		return 2, err
	}
	defer file.Close()

	buffer := make([]byte, 1)
	_, err = file.Read(buffer)
	if err != nil {
		return 2, fmt.Errorf("failed to read first byte: %w", err)
	}
	return buffer[0], nil
}

func (w *Wal) ReadAllSegments() ([]*Record, error) {
	var allRecords []*Record
	for _, segmentName := range w.ReadSegmentNames() {
		segmentPath := filepath.Join(w.DirectoryPath, segmentName)

		records, err := w.ReadSegmentFromFile(segmentPath)
		if err != nil {
			fmt.Printf("Error reading records from segment %s: %v\n", segmentPath, err)
			continue
		}
		allRecords = append(allRecords, records...)
	}
	noZerosRecords := NoZerosRecords(allRecords)
	defragmentedRecords := DefragmentRecords(noZerosRecords)
	return defragmentedRecords, nil
}

func NoZerosRecords(r []*Record) []*Record {
	for i:=0; i<len(r); i++ {
		r[i].Value = TrimZeros(r[i].Value)
		r[i].ValueSize = uint64(len(r[i].Value))
	}
	return r
}

func DefragmentRecords(r []*Record) []*Record {
	var temp []*Record 
	record := NewRecord("", []byte(""))
	for i:=0; i< len(r); i++ {
		if r[i].Type == 'a' {
			temp = append(temp, r[i])
		} else if r[i].Type == 'f' {
			record.Crc = r[i].Crc
			record.Timestamp = r[i].Timestamp
			record.Tombstone = r[i].Tombstone
			record.Type = 'a'
			record.KeySize = r[i].KeySize
			record.ValueSize += r[i].ValueSize
			record.Key = r[i].Key
			record.Value = append(record.Value, r[i].Value...)
		}
		if r[i].Type == 'm' {
			record.Value = append(record.Value, r[i].Value...  )
			record.ValueSize += r[i].ValueSize
		} else if r[i].Type == 'l' {
			record.Value = append(record.Value, r[i].Value...)
			record.ValueSize += r[i].ValueSize
			temp = append(temp, record)
			record = NewRecord("", []byte(""))
		}
	}
	return temp
}

// pad sistema
// povezi i dodaj funkcionalnost prebacivanja memtabeli i funkcionalnost za mijenjanje onih 8 bajtova kod potvrde sstabele