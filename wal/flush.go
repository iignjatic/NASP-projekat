package wal

import (
	"NASP-PROJEKAT/data"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
)

const FLUSH_META_FILE = "../wal/flush_meta.json"

type Flush struct {
	SegmentStart int `json:"segment_start"`
	OffsetStart  int `json:"offset_start"`
	SegmentEnd   int `json:"segment_end"`
	OffsetEnd    int `json:"offset_end"`
}

type FlushCheckpoint struct {
	SegmentID int `json:"segment_id"`
	Offset    int `json:"offset"`
}

func SaveFlushInfoToFile(info Flush) error {
	var infos []Flush

	file, err := os.OpenFile(FLUSH_META_FILE, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, _ := file.Stat()
	if stat.Size() > 0 {
		decoder := json.NewDecoder(file)
		err := decoder.Decode(&infos)
		if err != nil {
			return err
		}
	}

	infos = append(infos, info)

	file.Truncate(0)
	file.Seek(0, 0)
	encoder := json.NewEncoder(file)
	return encoder.Encode(infos)
}

func LoadFlushInfoFromFile() ([]Flush, error) {
	var infos []Flush

	file, err := os.Open(FLUSH_META_FILE)
	if err != nil {
		if os.IsNotExist(err) {
			return infos, nil // file doesn't exist, return empty slice
		}
		return nil, err
	}
	defer file.Close()

	// check if the file is empty
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if stat.Size() == 0 {
		return infos, nil // file exists but it is empty
	}

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&infos)
	if err != nil {
		return nil, err
	}

	return infos, nil
}

func LoadCheckpointFromFlushInfo() (FlushCheckpoint, error) {
	infos, err := LoadFlushInfoFromFile()
	if err != nil {
		return FlushCheckpoint{}, err
	}
	if len(infos) == 0 {
		return FlushCheckpoint{SegmentID: 0, Offset: 1}, nil
	}
	last := infos[len(infos)-1]
	return FlushCheckpoint{SegmentID: last.SegmentEnd, Offset: last.OffsetEnd}, nil
}

func (w *Wal) CreateFlushInfo(flushedRecords []*data.Record) (Flush, error) {
	if len(flushedRecords) == 0 {
		return Flush{}, fmt.Errorf("flushed records empty")
	}

	// find the record with the greatest timestamp and set it as last
	last, err := FindRecordWithMaxTimestamp(flushedRecords)
	if err != nil {
		return Flush{}, err
	}
	lastKey := FragmentKey{Key: last.Key, Timestamp: last.Timestamp}

	// find the record with the smallest timestamp in the range from the beginning to the last
	minKey, err := FindMinKeyBeforeLast(w.recordPositions, lastKey)
	if err != nil {
		return Flush{}, err
	}

	// create positions
	startPositions, ok := w.recordPositions[minKey]
	if !ok || len(startPositions) == 0 {
		return Flush{}, fmt.Errorf("there are no records for the first key : %s", minKey.Key)
	}

	endPositions, ok := w.recordPositions[lastKey]
	if !ok || len(endPositions) == 0 {
		return Flush{}, fmt.Errorf("there are no records for the last key: %s", lastKey.Key)
	}

	start := startPositions[0]  // 'f' fragment or the whole record
	end := endPositions[len(endPositions)-1]  // 'l' fragment or the whole record
	endOffset := end.Offset + end.Size  // end of the last fragment

	return Flush{
		SegmentStart: start.SegmentID,
		OffsetStart:  start.Offset,
		SegmentEnd:   end.SegmentID,
		OffsetEnd:    endOffset,
	}, nil
}

// Function to find the smallest key before the last key (up to 'last')
func FindMinKeyBeforeLast(recordPositions map[FragmentKey][]Position, lastKey FragmentKey) (FragmentKey, error) {
	var minKey FragmentKey
	minTimestamp := uint64(math.MaxUint64) // set the max uint64 number

	// conversion from string to int
	lastTimestamp, err := strconv.ParseUint(lastKey.Timestamp, 10, 64)
	if err != nil {
		return FragmentKey{}, fmt.Errorf("error converting lastKey.Timestamp for key %s: %v", lastKey.Key, err)
	}

	// go through all the recordPositions
	for key := range recordPositions {
		// convert key.Timestamp as well
		timestamp, err := strconv.ParseUint(key.Timestamp, 10, 64)
		if err != nil {
			return FragmentKey{}, fmt.Errorf("error converting timestamp for key %s: %v", key.Key, err)
		}

		// compare timestamps
		if timestamp < lastTimestamp && timestamp < minTimestamp {
			minKey = key
			minTimestamp = timestamp
		}
	}

	// not found
	if minTimestamp == uint64(math.MaxUint64) {
		return FragmentKey{}, fmt.Errorf("no smallest key found before the last one")
	}

	return minKey, nil
}

func FindRecordWithMaxTimestamp(records []*data.Record) (*data.Record, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("the list is empty")
	}
	var maxRecord *data.Record
	maxTimestamp := uint64(0)

	for _, record := range records {
		timestamp, err := strconv.ParseUint(record.Timestamp, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("error converting timestamp for key %s: %v", record.Key, err)
		}
		if timestamp > maxTimestamp {
			maxTimestamp = timestamp
			maxRecord = record
		}
	}
	return maxRecord, nil
}