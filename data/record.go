package data

type Record struct {
	crc        uint32
	key_size   uint32
	value_size uint32
	key        string
	value      []byte
	tombstone  bool
	timestamp  string
}
