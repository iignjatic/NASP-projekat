package data

const CRC_SIZE = 4
const TIMESTAMP_SIZE = 8
const TOMBSTONE_SIZE = 1
const TYPE_SIZE = 1
const KEY_SIZE = 8
const VALUE_SIZE = 8

type Record struct {
	Crc       uint64
	KeySize   uint64
	ValueSize uint64
	Key       string
	Value     []byte
	Type      byte
	Tombstone bool
	Timestamp string
}
