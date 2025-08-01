package memtable

import (
	"NASP-PROJEKAT/data"
	"errors"
)

type Memtable interface {
	Get(key string) (*data.Record, bool, bool)
	Delete(record *data.Record) ([]*data.Record, bool, error)
	Put(record *data.Record) ([]*data.Record, bool, error)
	LoadFromWal(records []*data.Record) ([][]*data.Record, error)
}

func CreateMemtableManager(memtableType string, maxTables, maxSize int) (Memtable, error) {
	switch memtableType {
	case "hashmap":
		return CreateMemtableManagerHM(uint(maxTables), uint(maxSize)), nil
	case "btree":
		return CreateMemtableManagerBTree(uint(maxTables), uint(maxSize)), nil
	case "skiplist":
		return CreateMemtableManagerS(uint(maxTables), uint(maxSize)), nil
	default:
		return nil, errors.New("unknown memtable type: " + memtableType)
	}
}
