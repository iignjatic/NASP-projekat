package wal

import (
	"NASP-PROJEKAT/data"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const DIRECTORY_PATH = "../wal/segments"

type FragmentKey struct {
	Key       string
	Timestamp string
}

type Position struct {
	SegmentID int
	Offset    int
	Size      int
}

type Wal struct {
	DirectoryPath  string
	Segments       []*Segment 
	CurrentSegment *Segment
	SegmentPaths   []string
	recordPositions map[FragmentKey][]Position
	blockSize         uint64
	blocksPerSegment  uint64
}

func NewWal(blockSize, blocksPerSegment uint64) *Wal {
	w := &Wal{
		DirectoryPath:  DIRECTORY_PATH,
		Segments:       []*Segment{},
		CurrentSegment: nil,
		SegmentPaths:   []string{},
		recordPositions: make(map[FragmentKey][]Position),
		blockSize: blockSize,
		blocksPerSegment: blocksPerSegment,
	}

	w.AddNewSegment()
	return w
}

func (w *Wal) AddNewSegment() {
	segmentNames, err := w.ReadSegmentNames()
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	w.SegmentPaths = segmentNames

	// get the last segment
	var lastSegment string
	if len(segmentNames) == 0 {
		newSegmentID := 0
		newSegment := NewSegment(newSegmentID, w.blockSize, w.blocksPerSegment)
		firstBlock := NewBlock(0, w.blockSize)
		newSegment.Blocks = append(newSegment.Blocks, firstBlock)
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
		records, err := w.ReadSegmentFromFile(segmentPath, 0, false, false)
		if err != nil {
			fmt.Printf("Error reading records from segment %s: %v\n", segmentPath, err)
			return
		}
		noZerosRecords := NoZerosRecords(records)
		// defragmentedRecords := DefragmentRecords(noZerosRecords)

		// create the segment and add records to it
		// last := w.SegmentPaths[len(w.SegmentPaths)-1]
		segmentID := ExtractSegmentNumber(lastSegment)
		lastUsedSegment := NewSegment(segmentID, w.blockSize, w.blocksPerSegment)
		w.Segments = append(w.Segments, lastUsedSegment)
		w.CurrentSegment = lastUsedSegment
		if len(lastUsedSegment.Blocks) == 0 {
			firstBlock := NewBlock(0, w.blockSize)
			lastUsedSegment.Blocks = append(lastUsedSegment.Blocks, firstBlock)
		}
		for _, record := range noZerosRecords {
			rec := data.NewRecord(record.Key, append([]byte{}, record.Value...))
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
		last := w.SegmentPaths[len(w.SegmentPaths)-1]
		newSegmentID := ExtractSegmentNumber(last) + 1
		newSegment := NewSegment(newSegmentID, w.blockSize, w.blocksPerSegment)
		firstBlock := NewBlock(0, w.blockSize)
		newSegment.Blocks = append(newSegment.Blocks, firstBlock)
		w.Segments = append(w.Segments, newSegment)
		w.CurrentSegment = newSegment
		w.SegmentPaths = append(w.SegmentPaths, newSegment.FilePath)
		fmt.Printf("Created new Segment %s\n", newSegment.FilePath)
	}
}

func (w *Wal) ReadSegmentNames() ([]string, error) {
	files, err := os.ReadDir(w.DirectoryPath)
	if err != nil {
		return nil, fmt.Errorf("error reading directory: %v", err)  
	}
	var segmentNames []string
	for _, file := range files {
		if !file.IsDir() {
			segmentName := file.Name()
			if ExtractSegmentNumber(segmentName) == -1 {
				return nil, fmt.Errorf("invalid segment name: %s", segmentName)
			}
			segmentNames = append(segmentNames, segmentName)
		}
	}

	// sort numerically - uses lambda anonymous function
	sort.Slice(segmentNames, func(i, j int) bool {
		return ExtractSegmentNumber(segmentNames[i]) < ExtractSegmentNumber(segmentNames[j])
	})

	return segmentNames, nil
}

// return number from segment name
func ExtractSegmentNumber(name string) int {
	parts := strings.TrimPrefix(name, "wal_")	// starts with wal_
	parts = strings.TrimSuffix(parts, ".bin") 	// ends with .bin
	num, err := strconv.Atoi(parts)
	if err != nil {
		return -1
	}
	return num
}

func (w *Wal) AddRecord(record *data.Record) {
	w.AddRecordToBlock(record)
}

func (w *Wal) FlushCurrentSegment() {
	if w.CurrentSegment != nil {
		w.WriteSegmentToFile(w.CurrentSegment)
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

	// reserve one byte for indicator of fullness of the segment
	currentOffset := 1
	offset := []byte{0}
	_, err = file.Write(offset)
	if err != nil {
		return err
	}

	if w.recordPositions == nil {
		w.recordPositions = make(map[FragmentKey][]Position)
	}

	for i:=0;i<len(s.Blocks);i++ {
		for j:=0;j<len(s.Blocks[i].Records);j++ {
			rec := s.Blocks[i].Records[j]

			recordBytes, err := rec.ToBytes()
			if err != nil {
				return err
			}
			_, err = file.Write(recordBytes)
			if err != nil {
				return err
			}

			// write positions
			key := FragmentKey{
				Key: rec.Key,
				Timestamp: rec.Timestamp,
			}
			pos := Position{
				SegmentID: s.ID,
				Offset:    currentOffset,
				Size:      len(recordBytes),
			}
			w.recordPositions[key] = append(w.recordPositions[key], pos)
			currentOffset += len(recordBytes)
		}
	}
	return nil
}

// just change the fist or the second byte to one if the segment is full or if the segment is sent to SSTable
func (w *Wal) WriteFirstByte(s *Segment) error {
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

// a function that reads the segment record by record
func (w *Wal) ReadSegmentFromFile(filePath string, offset int64, useCheckpoint bool, revoked bool) ([]*data.Record, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	if !useCheckpoint {
		offset = 1
	}

	segmentID := ExtractSegmentNumber(filepath.Base(filePath))

	// move the pointer to the first byte - first byte is for fullness of the file
	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to the first byte: %w", err)
	}

	var records []*data.Record
	buffer := make([]byte, w.blockSize)
	var data1 []byte
	currentOffset := int(offset)

	for {
		n, err := file.Read(buffer)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read file: %w", err)
		}
		if n == 0 {
			break
		}
		
		// handles leftovers
		data1 = append(data1, buffer[:n]...)

		i:=0
		for i<len(data1){
			for i < len(data1) && data1[i] == 0 {
				i++
			}
			minHeaderSize := data.KEY_START
			if len(data1[i:]) < minHeaderSize {
				break
			}

			keySize := binary.LittleEndian.Uint64(data1[i+data.KEY_SIZE_START:])
			valueSize := binary.LittleEndian.Uint64(data1[i+data.VALUE_SIZE_START:])
			totalSize := data.KEY_START + int(keySize) + int(valueSize)
			
			j := i
			for j+totalSize < len(data1) && data1[j+totalSize] == 0 {
				j++
				currentOffset++
			}

			if len(data1[i:]) < totalSize {
				break
			}

			record, err := data.FromBytes(data1[i:])
			if err != nil {
				return nil, fmt.Errorf("failed to parse record at position %d: %w",i, err)
			}
			records = append(records, record)

			recordBytes, err := record.ToBytes()
			if err != nil {
				return nil, fmt.Errorf("failed to serialize record to bytes: %w", err)
			}
			if revoked {
				key := FragmentKey{
					Key:       record.Key,
					Timestamp: record.Timestamp,
				}
				pos := Position{
					SegmentID: segmentID,
					Offset:    currentOffset,
					Size:      len(recordBytes),
				}
				w.recordPositions[key] = append(w.recordPositions[key], pos)
			}
			currentOffset += len(recordBytes)
			i += len(recordBytes)
		}
		data1 = data1[i:]
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

func (w *Wal) ReadAllSegmentsCP(rev bool) ([]*data.Record, error) {
	var allRecords []*data.Record

	// load the checkpoint
	cp, err := LoadCheckpointFromFlushInfo()
	if err != nil {
		return nil, fmt.Errorf("cannot laod the checkpoint: %w", err)
	}

	// read all segments
	segmentNames, err := w.ReadSegmentNames()
	if err != nil {
		return nil, err
	}

	// go through all the segments
	for _, segmentName := range segmentNames {
		segmentID := ExtractSegmentNumber(segmentName)
		segmentPath := filepath.Join(w.DirectoryPath, segmentName)

		if segmentID < cp.SegmentID {
			// segment is fully flushed, skip
			continue
		}

		var records []*data.Record
		if segmentID == cp.SegmentID {
			// segment is partially flushed
			records, err = w.ReadSegmentFromFile(segmentPath, int64(cp.Offset), true, rev)
		} else {
			// read the whole segment
			records, err = w.ReadSegmentFromFile(segmentPath, 0 , false, rev)
		}

		if err != nil {
			fmt.Printf("Error reading the segment %s: %v\n", segmentPath, err)
			continue
		}

		allRecords = append(allRecords, records...)
	}

	noZerosRecords := NoZerosRecords(allRecords)
	defragmentedRecords := DefragmentRecords(noZerosRecords)

	return defragmentedRecords, nil
}

func NoZerosRecords(r []*data.Record) []*data.Record {
	for i:=0; i<len(r); i++ {
		r[i].Value = TrimZeros(r[i].Value)
		r[i].ValueSize = uint64(len(r[i].Value))
	}
	return r
}

func NoZerosRecord(r *data.Record) *data.Record {
	r.Value = TrimZeros(r.Value)
	r.ValueSize = uint64(len(r.Value))
	return r
}

func DefragmentRecords(r []*data.Record) []*data.Record {
	var temp []*data.Record 
	record := data.NewRecord("", []byte(""))
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
			_, err := record.ToBytes()
			if err != nil {
				fmt.Printf("Error recalculating crc in record defragmentation: %v\n", err)
			}
			temp = append(temp, record)
			record = data.NewRecord("", []byte(""))
		}
	}
	return temp
}

func (w *Wal) DeleteFullyFlushedSegments(info Flush) error {
	for i := 0; i < len(w.SegmentPaths); {
		path := w.SegmentPaths[i]
		id := ExtractSegmentNumber(path)
		if id < 0 {
			i++
			continue
		}

		fullPath := filepath.Join(w.DirectoryPath, path)

		shouldDelete := false

		if id < info.SegmentEnd {
			shouldDelete = true
		} else if id == info.SegmentEnd {
			fileInfo, err := os.Stat(fullPath)
			if err != nil {
				fmt.Printf("error checking %s: %v\n", path, err)
				i++
				continue
			}
			if info.OffsetEnd >= int(fileInfo.Size()) {
				shouldDelete = true
			}
		}

		if shouldDelete {
			err := os.Remove(fullPath)
			if err != nil {
				fmt.Printf("error deleting %s: %v\n", path, err)
			} else {
				fmt.Printf("Segment deleted: %s\n", path)

				w.SegmentPaths = append(w.SegmentPaths[:i], w.SegmentPaths[i+1:]...)
				continue
			}
		}
		i++
	}
	return nil
}