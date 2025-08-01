package data

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"strconv"
	"time"
)

const (
	CRC_SIZE       = 4
	TIMESTAMP_SIZE = 8
	TOMBSTONE_SIZE = 1
	TYPE_SIZE      = 1
	KEY_SIZE       = 8
	VALUE_SIZE     = 8

	CRC_START        = 0
	TIMESTAMP_START  = CRC_START + CRC_SIZE
	TOMBSTONE_START  = TIMESTAMP_START + TIMESTAMP_SIZE
	TYPE_START       = TOMBSTONE_START + TOMBSTONE_SIZE
	KEY_SIZE_START   = TYPE_START + TYPE_SIZE
	VALUE_SIZE_START = KEY_SIZE_START + KEY_SIZE
	KEY_START        = VALUE_SIZE_START + VALUE_SIZE
)

/*
   +---------------+-----------------+---------------+------+---------------+----------+-------+-...-+--...--+
   |    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Type(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+------+---------------+----------+-------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value
   Value Size = Length of the Value data
	 Type = a - ALL, f - FIRST, m - MIDDLE, l - LAST,
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

type Record struct {
	Crc       uint32
	Timestamp string
	Tombstone bool
	Type      byte
	KeySize   uint64
	ValueSize uint64
	Key       string
	Value     []byte
}

func NewRecord(key string, value []byte) *Record {
	timestamp := fmt.Sprintf("%d", time.Now().UTC().UnixNano())
	rec := &Record{
		Crc:       0, // computed during serialization
		Timestamp: timestamp,
		Tombstone: false, // default value
		Type:      'a',   // default value - ALL
		KeySize:   uint64(len(key)),
		ValueSize: uint64(len(value)),
		Key:       key,
		Value:     value,
	}
	rec.ToBytes()
	return rec
}

func CRC32(data []byte) uint32 {
	// fmt.Printf("CRC input data: %v\n", data)
	crc := crc32.ChecksumIEEE(data)
	// fmt.Printf("Calculated CRC: %d\n", crc)
	return crc
}

func (r *Record) ToBytes() ([]byte, error) {
	keySize := len(r.Key)
	valueSize := len(r.Value)

	realValueSize := len(TrimZeros(r.Value))
	totalCRCValue := KEY_START + keySize + realValueSize

	// Compute total size of the byte array
	totalSize := KEY_START + keySize + valueSize

	bytesArray := make([]byte, totalSize)
	// Placeholder for CRC - computed later
	binary.LittleEndian.PutUint32(bytesArray[CRC_START:], 0)

	// Serialize Timestamp
	timestampInt, err := strconv.ParseUint(r.Timestamp, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp format: %v", err)
	}
	binary.LittleEndian.PutUint64(bytesArray[TIMESTAMP_START:], timestampInt)

	// Serialize Tombstone
	if r.Tombstone {
		bytesArray[TOMBSTONE_START] = 1
	} else {
		bytesArray[TOMBSTONE_START] = 0
	}

	// Serialize Type
	bytesArray[TYPE_START] = r.Type

	// Serialize KeySize and ValueSize
	binary.LittleEndian.PutUint64(bytesArray[KEY_SIZE_START:], uint64(keySize))
	binary.LittleEndian.PutUint64(bytesArray[VALUE_SIZE_START:], uint64(realValueSize))

	// Serialize Key and Value
	copy(bytesArray[KEY_START:], r.Key)
	copy(bytesArray[KEY_START+keySize:], r.Value)

	// Compute CRC
	crc := CRC32(bytesArray[CRC_SIZE:totalCRCValue])
	r.Crc = crc
	binary.LittleEndian.PutUint32(bytesArray[CRC_START:], crc)

	return bytesArray, nil
}

func checkOffset(offset, size, totalSize int, fieldName string) error {
	if offset+size > totalSize {
		return fmt.Errorf("insufficient data for %s: need %d bytes, but only %d bytes available", fieldName, size, totalSize-offset)
	}
	return nil
}

func IsValidType(t byte) bool {
	return t == 'a' || t == 'f' || t == 'm' || t == 'l'
}

func FromBytes(data []byte) (*Record, error) {
	// Deserialize CRC
	if err := checkOffset(CRC_START, CRC_SIZE, len(data), "CRC"); err != nil {
		return nil, err
	}
	crc := binary.LittleEndian.Uint32(data[CRC_START:])

	// Deserialize Timestamp
	if err := checkOffset(TIMESTAMP_START, TIMESTAMP_SIZE, len(data), "TIMESTAMP"); err != nil {
		return nil, err
	}
	timestampInt := int64(binary.LittleEndian.Uint64(data[TIMESTAMP_START:]))
	timestamp := fmt.Sprintf("%d", timestampInt)

	// Deserialize Tombstone
	if err := checkOffset(TOMBSTONE_START, TOMBSTONE_SIZE, len(data), "TOMBSTONE"); err != nil {
		return nil, err
	}
	tombstone := data[TOMBSTONE_START] == 1

	// Deserialize Type
	if err := checkOffset(TYPE_START, TYPE_SIZE, len(data), "TYPE"); err != nil {
		return nil, err
	}
	typeField := data[TYPE_START]
	if !IsValidType(typeField) {
		return nil, fmt.Errorf("invalid type: %c", typeField)
	}

	// Deserialize KeySize
	if err := checkOffset(KEY_SIZE_START, KEY_SIZE, len(data), "KEY_SIZE"); err != nil {
		return nil, err
	}
	keySize := binary.LittleEndian.Uint64(data[KEY_SIZE_START:])

	// Deserialize ValueSize
	if err := checkOffset(VALUE_SIZE_START, VALUE_SIZE, len(data), "VALUE_SIZE"); err != nil {
		return nil, err
	}
	valueSize := binary.LittleEndian.Uint64(data[VALUE_SIZE_START:])

	// Deserialize Key
	keyStart := KEY_START
	if err := checkOffset(keyStart, int(keySize), len(data), "KEY"); err != nil {
		return nil, err
	}
	key := string(data[keyStart : keyStart+int(keySize)])

	// Deserialize Value
	valueStart := KEY_START + int(keySize)
	if err := checkOffset(valueStart, int(valueSize), len(data), "VALUE"); err != nil {
		return nil, err
	}
	value := data[valueStart : valueStart+int(valueSize)]

	realValueSize := len(TrimZeros(value))
	// Compare old and new CRC
	calculatedCrc := CRC32(data[CRC_SIZE : valueStart+int(realValueSize)])
	if crc != calculatedCrc {
		return nil, fmt.Errorf("crc mismatch: expected %d, got %d", crc, calculatedCrc)
	}

	return &Record{
		Crc:       crc,
		Timestamp: timestamp,
		Tombstone: tombstone,
		Type:      typeField,
		KeySize:   keySize,
		ValueSize: valueSize,
		Key:       key,
		Value:     value,
	}, nil
}

func CalculateRecordSize(record *Record) int {
	return KEY_START + len(record.Key) + len(record.Value)
}

func TrimZeros(data []byte) []byte {
	for len(data) > 0 && data[len(data)-1] == 0 {
		data = data[:len(data)-1]
	}
	return data
}

func DeepCopyRecord(original *Record) *Record {
	if original == nil {
		return nil
	}

	copyValue := make([]byte, len(original.Value))
	copy(copyValue, original.Value)

	return &Record{
		Crc:       original.Crc,
		Timestamp: original.Timestamp,
		Tombstone: original.Tombstone,
		Type:      original.Type,
		KeySize:   original.KeySize,
		ValueSize: original.ValueSize,
		Key:       original.Key,
		Value:     copyValue,
	}
}
