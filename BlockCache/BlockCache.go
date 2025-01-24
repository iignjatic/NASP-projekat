package BlockCache

type BlockNode struct {
	Block *Block
	Prev *BlockNode
	Next *BlockNode
}

type LRUlist struct {
	Head *BlockNode
	Tail *BlockNode
}

type BlockCache struct {
	Capacity uint32
	LRUlist  *LRUlist
	BlockMap map[uint32]*BlockNode
}

func (cache *BlockCache) checkCache(key uint32) block *Block{
	_, exists := cache.blockMap[int(key)]
	if exists {
		return cache.blockMap.block
	} else {
		return nil
	}
} 

func (cache *BlockCache) addCache(key uint32, block *Block){
	node, exists := cache.blockMap[int(key)]
	if exists {
		node.Prev.Next = node.Next
		node.Next.Prev = node.Prev
		node.Next = cache.LRUlist.Head.Next
		cache.LRUlist.Head = newNode
		
	} else {
		newNode := &BlockNode{
			Block: block,
			Prev:  nil,
			Next:  cache.LRUlist.head,
		}
		newNode.Next = cache.LRUlist.Head.Next
		cache.LRUlist.Head = newNode

		LRUlist.Tail.Prev.Next = nil
		delete(cache.BlockMap, cache.LRUlist.Tail.Block.Key)


		
	}
}