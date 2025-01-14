package data

type Record struct {
	Crc       uint32
	KeySize   uint32
	ValueSize uint32
	Key       string
	Value     []byte
	Tombstone bool
	TimeStamp string
}
