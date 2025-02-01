package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
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
	newSegmentID := len(w.Segments)
	newSegment := NewSegment(newSegmentID)
	w.Segments = append(w.Segments, newSegment)
	w.CurrentSegment = newSegment
	s := fmt.Sprint(w.DirectoryPath,"/",newSegment.FilePath)
	w.SegmentPaths = append(w.SegmentPaths, s)
	fmt.Printf("Created new Segment %s\n", newSegment.FilePath)
	w.PrintCurrentWalSegmentsIDs()
}

func (w *Wal) AddRecord(record *Record) {
	w.CurrentSegment.AddRecordToBlock(record)
	if w.CurrentSegment.IsFull() {
		w.FlushCurrentSegment()
		w.AddNewSegment()
	}
	w.FlushCurrentSegment()
}

func (w *Wal) FlushCurrentSegment() {
	if w.CurrentSegment != nil {
		// fmt.Printf("Flushing segment %s to disk...\n", w.CurrentSegment.FilePath)
		w.WriteSegmentToFile(w.CurrentSegment)
		w.CurrentSegment.Transferred = true
	}
}

// a function that writes records to the segment file
func (w *Wal) WriteSegmentToFile(s *Segment) error {
	filePath := w.DirectoryPath + "/" + s.FilePath
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// reserve eight bytes for number of bytes that are pushed on memtable from this segment
	offset := make([]byte, 8)
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

// a function that reads the whole segment record by record, or from a particular position in the file
func (w *Wal) ReadSegmentFromFile(filePath string) ([]*Record, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to opet file %s: %w", filePath, err)
	}
	defer file.Close()
	
	// read offset
	offsetBytes := make([]byte, 8)
	_, err = file.Read(offsetBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to read offset: %w", err)
	}

	offset := binary.LittleEndian.Uint64(offsetBytes) // convert offset to uint64

	// set the position to offset
	if offset != 0 {
		_, err := file.Seek(int64(offset), 0)
		if err != nil {
			return nil, fmt.Errorf("failed to seek to offset %d: %w", offset, err)
		}
	}

	var records []*Record
	buffer := make([]byte, BLOCK_SIZE)

	for {
		n, err := file.Read(buffer)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read file: %w", err)
		}
		if n == 0 {
			break
		}

		i:=0
		for i<n {
			record, err := FromBytes(buffer[i:])
			if err != nil {
				return nil, fmt.Errorf("failed to parse record: %w", err)
			}
			records = append(records, record)
			recordBytes, err := record.ToBytes()
			if err != nil {
				return nil, fmt.Errorf("failed to serialize record to bytes: %w", err)
			}
			i += len(recordBytes)
		}
	}
	// recordsTemp := DefragmentRecords(records)
	return records, nil
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
			record.ValueSize = r[i].ValueSize
			record.Key = r[i].Key
			record.Value = append(record.Value, r[i].Value...)
		}
		if r[i].Type == 'm' {
			record.Value = append(record.Value, r[i].Value...)
		} else if r[i].Type == 'l' {
			record.Value = append(record.Value, r[i].Value...)
			temp = append(temp, record)
			record = NewRecord("", []byte(""))
		}
	}
	return temp
}

func (w *Wal) PrintRecordsFromSegments() {
	for _, segmentPath := range w.SegmentPaths {
		records, err := w.ReadSegmentFromFile(segmentPath)
		if err != nil {
			fmt.Printf("Error reading records from segment %s: %v\n", segmentPath, err)
			continue
		}

		fmt.Printf("Records from sefment %s:\n", segmentPath)
		for _, record := range records {
			fmt.Println(record)
		}
	}
}


// func (w *Wal) RemoveSegment(ID int) []*Segment {
// 	return append(w.Segments[:ID], w.Segments[ID+1:]...)
// }

func (w *Wal) PrintCurrentWalSegmentsIDs() {
	fmt.Print("Current Wal Segment IDs: ")
	for i:=0;i<len(w.Segments);i++ { 
		fmt.Printf("%d ", w.Segments[i].ID)
	}
	fmt.Println()
}