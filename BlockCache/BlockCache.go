package BlockCache

import (
	"NASP-PROJEKAT/data"
)

type BlockNode struct {
	Block *data.Block
	Key   string //redni broj bloka
	Prev  *BlockNode
	Next  *BlockNode
}

type LRUlist struct {
	Head *BlockNode
	Tail *BlockNode
}

type BlockCache struct {
	Capacity uint32
	LRUlist  *LRUlist
	BlockMap map[string]*BlockNode
}

func (cache *BlockCache) CheckCache(key string) *data.Block {
	_, exists := cache.BlockMap[key]
	if exists {
		return cache.BlockMap[key].Block
	} else {
		return nil
	}
}

func (cache *BlockCache) AddCache(key string, block *data.Block) {
	node, exists := cache.BlockMap[key]
	if exists {
		if cache.LRUlist.Head != node {
			node.Prev.Next = node.Next
			node.Next.Prev = node.Prev
			node.Next = cache.LRUlist.Head.Next
			cache.LRUlist.Head.Prev = node
			cache.LRUlist.Head = node
		}

	} else {
		newNode := &BlockNode{
			Block: block,
			Key:   key,
			Prev:  nil,
			Next:  nil,
		}
		if cache.LRUlist.Head == nil {
			cache.LRUlist.Head = newNode
			cache.LRUlist.Tail = newNode

		} else if cache.LRUlist.Head == cache.LRUlist.Tail {
			newNode.Next = cache.LRUlist.Head
			cache.LRUlist.Head = newNode
			cache.LRUlist.Tail.Prev = newNode

		} else {
			newNode.Next = cache.LRUlist.Head
			cache.LRUlist.Head.Prev = newNode
			cache.LRUlist.Head = newNode
			if cache.Capacity == 0 {
				delete(cache.BlockMap, cache.LRUlist.Tail.Key)
				cache.LRUlist.Tail = cache.LRUlist.Tail.Prev
				cache.LRUlist.Tail.Next = nil
			}
		}
		cache.BlockMap[key] = newNode
	}
	if cache.Capacity != 0 {
		cache.Capacity--
	}
}
