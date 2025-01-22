package wal

import (
	"fmt"
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
	w.SegmentPaths = append(w.SegmentPaths, newSegment.FilePath)
	fmt.Printf("Created new Segment %s\n", newSegment.FilePath)
	w.PrintCurrentWalSegmentsID()
}

func (w *Wal) AddRecord(record *Record) {
	w.CurrentSegment.AddRecordToBlock(record)
	if w.CurrentSegment.IsFull() {
		w.FlushCurrentSegment()
		w.AddNewSegment()
	}
}

func (w *Wal) FlushCurrentSegment() {
	if w.CurrentSegment != nil {
		fmt.Printf("Flushing segment %s to disk...\n", w.CurrentSegment.FilePath)
		w.WriteSegmentToFile(w.CurrentSegment)
		w.CurrentSegment.Transferred = true
	}
}

func (w *Wal) WriteSegmentToFile(s *Segment) error {
	filePath := w.DirectoryPath + "/" + s.FilePath
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

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

// func (w *Wal) RemoveSegment(ID int) []*Segment {
// 	return append(w.Segments[:ID], w.Segments[ID+1:]...)
// }

func (w *Wal) PrintCurrentWalSegmentsID() {
	fmt.Print("Current Wal Segment IDs: ")
	for i:=0;i<len(w.Segments);i++ { 
		fmt.Printf("%d ", w.Segments[i].ID)
	}
	fmt.Println()
}