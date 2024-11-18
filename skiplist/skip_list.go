package skiplist

import (
	"math/rand"
)

type Node struct {
	key   string
	value string
	next  []*Node
}

type SkipList struct {
	head      *Node
	maxHeight int
}

/*
funkcija za odredjivanje broja nivoa elementa
*/
func (s *SkipList) roll() int {
	level := 0
	// possible ret values from rand are 0 and 1
	// we stop shen we get a 0
	for ; rand.Int31n(2) == 1; level++ {
		if level >= s.maxHeight {
			return level
		}
	}
	return level
}

/*
funkcija za dodavanje elemenata u skip listu
parametri: kljuc i vrijednost
povratna vrijednost: nil

prvo se odredjuje broj nivoa na kojima ce se pojavljivati cvor
dalje se prevezuju pokazivaci
*/
func (s *SkipList) addElement(key string, value string) {
	levels := s.roll()
	newNode := &Node{
		key:   key,
		value: value,
		next:  make([]*Node, levels+1),
	}
	current := s.head
	for i := levels; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key < newNode.key {
			current = current.next[i]
		}
		newNode.next[i] = current.next[i]
		current.next[i] = newNode
	}
}

/*
funkcija za pretragu u skiplisti
parametri: kljuc koji se trazi
povratna vrijednost: ako je nadjen onda cvor ako ne onda nil
*/
func (s *SkipList) searchElement(key string) *Node {
	current := s.head
	levels := len(current.next)
	for i := levels - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
		if current.next[i] != nil && current.next[i].key == key {
			return current.next[i]

		}
	}
	return nil

}

/*
funkcija za brisanje elementa iz skip liste
parametri: kljuc koji brisemo
povratna vrijednost: nil

ideja: krecem s vrha i zapamtim cvorove koji su potencijalno neposredno ispred zeljenog za brisanje
poslije provjerim koji od njih su
*/
func (s *SkipList) removeElement(key string) {
	current := s.head
	levels := len(current.next)

	for i := levels - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
		if current.next[i] != nil && current.next[i].key == key {
			current.next[i] = current.next[i].next[i]

		}
	}

}

/*
func main() {
	s := SkipList{head: &Node{next: make([]*Node, 4)}, maxHeight: 3}
	s.addElement("key1", "value1")
	s.addElement("key2", "value2")
	s.addElement("key3", "value3")

	//key := "key8"
	key := "key3"

	node := s.searchElement(key)
	if node != nil {
		fmt.Printf("Found key: %s, value %s", node.key, node.value)
		fmt.Println()
	} else {
		fmt.Printf("Key %s not found\n", key)
	}

	s.removeElement("key3")
	node2 := s.searchElement("key3")
	fmt.Println(node2)
}
*/
