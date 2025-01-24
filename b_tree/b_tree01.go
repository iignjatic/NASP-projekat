package main

import (
	"errors"
	"fmt"
	"time"
)

type Record struct {
	key       string
	value     []byte
	tombstone bool
	timestamp string
}

type BTreeNode struct {
	keys     []string     //niz kljuceva
	records  []*Record    //odgovarajuci record-i za kljuceve
	children []*BTreeNode //niz djece
	isLeaf   bool         //true ako je cvor list
	numKeys  int          //trenutni broj kljuceva u cvoru
}

type BTree struct {
	root *BTreeNode
	m    int //minimalan broj kljuceva u cvoru
}

func NewBTreeNode(m int, leaf bool) *BTreeNode {
	return &BTreeNode{
		keys:     make([]string, 0, 2*m-1),
		records:  make([]*Record, 0, 2*m-1),
		children: make([]*BTreeNode, 0, 2*m-1),
		isLeaf:   leaf,
		numKeys:  0,
	}
}

func NewBTree(m int) *BTree {
	return &BTree{
		root: NewBTreeNode(m, true),
		m:    m,
	}
}

func (tree *BTree) PrintTree(node *BTreeNode, level int) {
	if node == nil {
		return
	}

	fmt.Printf("Level %d: %d keys\n", level, len(node.keys))
	for _, k := range node.keys {
		fmt.Printf("%s ", k)
	}
	fmt.Println()

	level++
	if len(node.children) > 0 {
		for _, c := range node.children {
			tree.PrintTree(c, level)
		}
	}
}

func (tree *BTree) Search(key string) (*BTreeNode, int) {
	return tree.searchRecursive(key, tree.root)
}

// mozda bi trebalo vratiti record u tom nodu, ali mozda i ne treba jer imam node i koji je po redu kljuc i record jer su u istom redoslijedu
func (tree *BTree) searchRecursive(key string, node *BTreeNode) (*BTreeNode, int) {
	if node == nil {
		return nil, -1
	}

	i := 0
	for i < len(node.keys) && key > node.keys[i] {
		i++
	}

	if i < len(node.keys) && key == node.keys[i] {
		return node, i
	}

	if node.isLeaf {
		return nil, -1
	}

	return tree.searchRecursive(key, node.children[i])
}

func (tree *BTree) Update(key string, newValue []byte) error {
	node, index := tree.Search(key)
	if index == -1 {
		return errors.New("key not found")
	}

	record := node.records[index]
	record.value = newValue
	record.timestamp = time.Now().UTC().Format(time.RFC3339)
	return nil
}

func (tree *BTree) Delete(key string) error {
	node, index := tree.Search(key)
	if index == -1 {
		return errors.New("key not found")
	}

	record := node.records[index]
	record.tombstone = true
	record.timestamp = time.Now().UTC().Format(time.RFC3339)
	return nil
}

func (tree *BTree) Insert(key string, value []byte) {
	root := tree.root
	if root.numKeys == 2*tree.m-1 {
		newRoot := NewBTreeNode(tree.m, false)
		tree.root = newRoot
		newRoot.children = append(newRoot.children, root)
		tree.splitChild(newRoot, 0)
		tree.insertNonFull(newRoot, key, value)
	} else {
		tree.insertNonFull(root, key, value)
	}
}

func (tree *BTree) insertNonFull(node *BTreeNode, key string, value []byte) {
	i := node.numKeys - 1

	if node.isLeaf {
		node.keys = append(node.keys[:i+1], node.keys[i:]...)
		node.keys[i+1] = key
		node.records = append(node.records[:i+1], node.records[i:]...)
		node.records[i+1] = &Record{
			key:       key,
			value:     value,
			timestamp: time.Now().UTC().Format(time.RFC3339),
			tombstone: false}
		node.numKeys++
	} else {
		for i >= 0 && key < node.keys[i] {
			i--
		}
		i++
		if node.children[i].numKeys == (2*tree.m)-1 {
			tree.splitChild(node, i)
			if key > node.keys[i] {
				i++
			}
		}
		tree.insertNonFull(node.children[i], key, value)
	}
}

func (tree *BTree) splitChild(node *BTreeNode, index int) {
	t := tree.m
	child := node.children[index]
	newChild := NewBTreeNode(t, child.isLeaf)
	newChild.numKeys = t - 1

	newChild.keys = append(newChild.keys, child.keys[t:]...)
	child.keys = child.keys[:t-1]
	if !child.isLeaf {
		newChild.children = append(newChild.children, child.children[t:]...)
		child.children = child.children[:t]
	}

	node.children = append(node.children[:index+1], append([]*BTreeNode{newChild}, node.children[index+1:]...)...)
	node.keys = append(node.keys[:index], append([]string{child.keys[t-1]}, node.keys[index:]...)...)
	node.numKeys++
}
