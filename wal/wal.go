package wal

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

// const (
// 	CRC_SIZE = 4
// 	TIMESTAMP_SIZE = 8
// 	TOMBSTONE_SIZE = 1
// 	KEY_SIZE_SIZE = 8
// 	VALUE_SIZE_SIZE = 8

// 	CRC_START = 0
// 	TIMESTAMP_START = CRC_START + CRC_SIZE
// 	TOMBSTONE_START = TIMESTAMP_START + TIMESTAMP_SIZE
// 	KEY_SIZE_START = TOMBSTONE_START + TOMBSTONE_SIZE
// 	VALUE_SIZE_START = KEY_SIZE_START + KEY_SIZE_SIZE
// 	KEY_START = VALUE_SIZE_START + VALUE_SIZE_SIZE
// )

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