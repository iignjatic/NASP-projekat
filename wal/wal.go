package wal

import (
	"NASP-PROJEKAT/data"
	"bufio"
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
	segmentNames, err := w.ReadSegmentNames()
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	w.SegmentPaths = segmentNames

	// get the last segment
	var lastSegment string
	if len(segmentNames) == 0 {
		newSegmentID := len(segmentNames)
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
		// last := w.SegmentPaths[len(w.SegmentPaths)-1]
		lastUsedSegment := NewSegment(len(w.SegmentPaths)-1)
		w.Segments = append(w.Segments, lastUsedSegment)
		w.CurrentSegment = lastUsedSegment
		for _, record := range defragmentedRecords {
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
		newSegment := NewSegment(newSegmentID)
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

func (w *Wal) AddRecord(record *data.Record) []*data.Record {
	var records []*data.Record
	// w.RecordToSegment[record] = w.CurrentSegment.ID
	// w.SegmentRecordCount[w.CurrentSegment.ID]++
	records = w.AddRecordToBlock(record)
	return records 
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
	offset := []byte{0}
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

// a function that reads the whole segment record by record, or from a particular position in the file
func (w *Wal) ReadSegmentFromFile(filePath string) ([]*data.Record, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	// move the pointer to the first byte - first byte is for fullness of the file
	_, err = file.Seek(1, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to the first byte: %w", err)
	}

	var records []*data.Record
	buffer := make([]byte, BLOCK_SIZE)
	var data1 []byte

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
		for i<len(data1) {
			if isAllZeros(data1[i:]) {
				return records, nil
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
			i += len(recordBytes)
		}
		data1 = data1[i:]
	}
	return records, nil
}

func isAllZeros(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
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

// for testing functionality of segmentation
func (w *Wal) ReadAllSegments() ([]*data.Record, error) {
	var allRecords []*data.Record
	segmentNames, err := w.ReadSegmentNames()
	if err != nil {
		return nil, err
	}
	if len(segmentNames) != 0 {
		for _, segmentName := range segmentNames {
			segmentPath := filepath.Join(w.DirectoryPath, segmentName)
			records, err := w.ReadSegmentFromFile(segmentPath)
			if err != nil {
				fmt.Printf("Error reading records from segment %s: %v\n", segmentPath, err)
				continue
			}
			allRecords = append(allRecords, records...)
		}
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
			temp = append(temp, record)
			record = data.NewRecord("", []byte(""))
		}
	}
	return temp
}

// Function to write two numbers to the first and second lines of the file
func WriteNumbersToFile(filename string, number1 int, number2 int) error {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	// Ensure there are at least two lines in the file
	if len(lines) < 2 {
		lines = append(lines, "", "") // Add two empty lines if they don't exist
	}
	// Write numbers to the first and second lines
	lines[0] = fmt.Sprintf("%d", number1) // First line
	lines[1] = fmt.Sprintf("%d", number2) // Second line
	// Rewrite the entire file
	file.Truncate(0)
	file.Seek(0, 0)
	writer := bufio.NewWriter(file)
	for _, line := range lines {
		_, err := writer.WriteString(line + "\n")
		if err != nil {
			return err
		}
	}
	writer.Flush()
	return nil
}

// Function to read two numbers from the first and second lines of the file
func ReadNumbersFromFile(filename string) (int, int, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	// Ensure that there are at least two lines in the file
	if len(lines) < 2 {
		return 0, 0, fmt.Errorf("file does not contain enough lines")
	}
	// Parse the numbers from the first and second lines
	number1, err := strconv.Atoi(lines[0])
	if err != nil {
		return 0, 0, err
	}
	number2, err := strconv.Atoi(lines[1])
	if err != nil {
		return 0, 0, err
	}
	return number1, number2, nil
}

func (w *Wal) DeleteFullyFlushedSegments(bytesStart int, bytesEnd int) error {
	segmentNames, err := w.ReadSegmentNames()
	if err != nil {
		return err
	}
	for _, segmentName := range segmentNames {
		segmentIndex := ExtractSegmentNumber(segmentName)
		segmentStart := segmentIndex * SEGMENT_SIZE
		segmentEnd := segmentStart + SEGMENT_SIZE
		// Delete fully included segments within the range
		if segmentStart >= bytesStart && segmentEnd <= bytesEnd {
			// Delete the segment
			err := w.DeleteSegment(segmentIndex)
			if err != nil {
				return err
			}
			continue
		}
		// If the last segment is partially included, check if it has data
		if segmentEnd > bytesEnd {
			// Check the remaining part of the last segment (if there is a remainder, check if it's empty)
			remainingBytesStart := bytesEnd
			remainingBytesEnd := segmentEnd
			hasData, err := w.HasDataInSegment(segmentName, remainingBytesStart, remainingBytesEnd)
			if err != nil {
				return err
			}
			if !hasData {
				// If no data, delete the segment
				err := w.DeleteSegment(ExtractSegmentNumber(segmentName))
				if err != nil {
					return err
				}
			}
		}
	}
	ok, err := w.IsDirectoryEmpty() 
	if err != nil {
		panic(err)
	}
	if ok {
		w.Segments = []*Segment{}
		w.CurrentSegment = nil
		w.SegmentPaths = []string{}
		w.AddNewSegment()
	} 
	return nil
}

// DeleteSegment deletes the segment file based on its ID.
func (w *Wal) DeleteSegment(segmentID int) error {
	segmentFile := fmt.Sprintf("wal_%d.bin", segmentID)
	path := filepath.Join(w.DirectoryPath, segmentFile)
	err := os.Remove(path)
	if err != nil {
		fmt.Printf("Error deleting segment %s: %v\n", segmentFile, err)
		return err
	}
	fmt.Printf("Segment %d successfully deleted.\n", segmentID)
	return nil
}

func (w *Wal) HasDataInSegment(segmentName string, bytesStart int, bytesEnd int) (bool, error) {
	path := filepath.Join(w.DirectoryPath, segmentName)
	file, err := os.Open(path)
	if err != nil {
		return false, fmt.Errorf("error opening segment %s: %v", segmentName, err)
	}
	defer file.Close()
	fileInfo, err := file.Stat()
	if err != nil {
		return false, fmt.Errorf("error getting file info for %s: %v", segmentName, err)
	}
	if fileInfo.Size() <= int64(bytesEnd) {
		return false, nil // No data after bytesEnd
	}
	return true, nil
}

func (w *Wal) IsDirectoryEmpty() (bool, error) {
	dir, err := os.Open(w.DirectoryPath)
	if err != nil {
		return false, fmt.Errorf("error opening directory %s: %v", w.DirectoryPath, err)
	}
	defer dir.Close()
	files, err := dir.Readdirnames(0) // 0 means to read all files
	if err != nil {
		return false, fmt.Errorf("error reading directory %s: %v", w.DirectoryPath, err)
	}
	return len(files) == 0, nil
}


func CalculateRecordsSize(r []*data.Record) int {
	var sum int
	for _, rec := range r {
		sum += data.CalculateRecordSize(rec)
	}
	return sum
} 