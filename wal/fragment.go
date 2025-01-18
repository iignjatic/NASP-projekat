package wal

type Fragment struct {
	Type      string // FIRST, MIDDLE, LAST
	Crc       uint32 // inherited from the record
	Timestamp int64  // inherited from the record
	Tombstone bool   // inherited from the record
	KeySize   uint64 // inherited from the record
	ValueSize uint64 // inherited from the record
	Key       string // inherited from the record
	Value     []byte // part of the value from the record
}

func FragmentRecord(record *Record, maxFragmentSize int) []*Fragment {
	var fragments []*Fragment
	remainingValue := record.Value
	remainingValueSize := len(remainingValue)

	// Calculate fragment count based on the value size and max fragment size
	fragmentCount := (remainingValueSize + maxFragmentSize - 1) / maxFragmentSize

	for i := 0; i < fragmentCount; i++ {
		var fragmentType string
		if i == 0 {
			fragmentType = "FIRST"
		} else if i == fragmentCount-1 {
			fragmentType = "LAST"
		} else {
			fragmentType = "MIDDLE"
		}

		currentFragmentSize := min(len(remainingValue), maxFragmentSize)

		fragments = append(fragments, &Fragment{
			Type:      fragmentType,
			Crc:       record.Crc,
			Timestamp: record.Timestamp,
			Tombstone: record.Tombstone,
			KeySize:   uint64(len(record.Key)),
			ValueSize: uint64(remainingValueSize),
			Key:       record.Key,
			Value:     remainingValue[:currentFragmentSize],
		})
		remainingValue = remainingValue[currentFragmentSize:]
	}
	return fragments
}

func CalculateFragmentSize(fragment *Fragment) int {
	return KEY_START + len(fragment.Key) + len(fragment.Value)
}