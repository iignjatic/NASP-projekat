package wal

type Segment struct {
	ID              int
	Blocks          []byte
	CurrentCapacity int
	FullCapacity    int
}
