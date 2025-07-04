package wal

import (
	"NASP-PROJEKAT/data"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

const FLUSH_META_FILE = "../wal/flush_meta.json"

type Flush struct {
	SegmentEnd int `json:"segment_end"`
	OffsetEnd  int `json:"offset_end"`
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

func LoadCheckpointFromFlushInfo() (Flush, error) {
	infos, err := LoadFlushInfoFromFile()
	if err != nil {
		return Flush{}, err
	}
	if len(infos) == 0 {
		return Flush{SegmentEnd: 0, OffsetEnd: 1}, nil
	}
	last := infos[len(infos)-1]
	return last, nil
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

	endPositions, ok := w.recordPositions[lastKey]
	if !ok || len(endPositions) == 0 {
		return Flush{}, fmt.Errorf("no records for last key: %s", lastKey.Key)
	}

	end := endPositions[len(endPositions)-1]
	endOffset := end.Offset + end.Size

	return Flush{
		SegmentEnd: end.SegmentID,
		OffsetEnd:  endOffset,
	}, nil
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
