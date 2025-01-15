package wal

// type Wal struct {
// 	Segments 			 []*Segment	// Array of Segments
// 	Directory 		 string 		// Directory where Segments are stored
// 	CurrentSegment *Segment		// Current active segment
// 	SegmentSize 	 int 				// Maximum number of blocks per Segment
// 	NextSegmentID  int				// ID of the next Segment
// }

// func NewWal(directory string, segmentSize int) *Wal {
// 	if _, err := os.Stat(directory); os.IsNotExist(err) {
// 		os.Mkdir(directory, os.ModePerm)
// 	}

// 	segmentPath := filepath.Join(directory, fmt.Sprint("wal_segment_%d.log", 0))
// 	initialSegment := NewSegment(0, segmentSize, segmentPath)

// 	return &Wal{
// 		Segments: 			[]*Segment{initialSegment},
// 		Directory:  		directory,
// 		CurrentSegment: initialSegment,
// 		SegmentSize: 		segmentSize,
// 		NextSegmentID: 	1,
// 	}
// }

// // Add Block to the WAL
// func (w *Wal) AddBlock(block *Block) {
// 	if w.CurrentSegment.CurrentCapacity == w.CurrentSegment.FullCapacity {
// 		w.CurrentSegment.FlushToDisk()
// 		segmentPath := filepath.Join(w.Directory, fmt.Sprintf("wal_segment_%d.log", w.NextSegmentID))
// 		newSegment := NewSegment(w.NextSegmentID, w.SegmentSize, segmentPath)
// 		w.Segments = append(w.Segments, newSegment)
// 		w.CurrentSegment = newSegment
// 		w.NextSegmentID++
// 	}
// 	w.CurrentSegment.AddBlock(block)
// }

// func (w *Wal) FlushToDisk() {
// 	fmt.Printf("Flushing all segments to disk...")
// 	for _, segment := range w.Segments {
// 		segment.FlushToDisk()
// 	}
// }