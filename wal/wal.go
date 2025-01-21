package wal

import "fmt"

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
		w.CurrentSegment.Transferred = true
	}
}