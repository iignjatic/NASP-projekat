package skiplist

import (
	"NASP-PROJEKAT/data"
	"math"
	"math/rand"
)

type Node struct {
	key   string
	value *data.Record
	next  []*Node
}

type SkipList struct {
	head      *Node
	maxHeight int
}

// konstruktor
func NewSkipList(capacity int) *SkipList {
	return &SkipList{
		head:      &Node{next: make([]*Node, capacity)},
		maxHeight: int(math.Sqrt(float64(capacity))),
	}
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
func (s *SkipList) addElement(key string, value *data.Record) {
	//update ako je vec u listi
	if s.searchElement(key) != nil {
		current := s.head
		for i := s.maxHeight; i >= 0; i-- {
			for current.next[i] != nil && current.next[i].key <= key {
				if current.next[i].key == key {
					current.next[i].key = key
					current.next[i].value = value
				}
				current = current.next[i]
			}
		}
		return
	}
	//ako nije pronadjen dodaje se u skiplistu
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

// sortiranje zapisa
// vraca niz sortiranih
func (s *SkipList) sortElements() []*data.Record {
	var records []*data.Record
	current := s.head.next[0]
	for current != nil {
		records = append(records, current.value)
		current = current.next[0]
	}
	return records
}

/*
func main() {
	s := NewSkipList(10)
	records := []data.Record{
		{Crc: 12345, KeySize: 4, ValueSize: 5, Key: "key1", Value: []byte("val1"), Tombstone: false, Timestamp: "2024-06-14"},
		{Crc: 67890, KeySize: 4, ValueSize: 200, Key: "key2", Value: []byte("val2"), Tombstone: true, Timestamp: "2024-06-15"},
		{Crc: 54321, KeySize: 4, ValueSize: 140, Key: "key3", Value: []byte("val3"), Tombstone: false, Timestamp: "2024-06-16"},
		{Crc: 12345, KeySize: 4, ValueSize: 5, Key: "key3", Value: []byte("val4"), Tombstone: false, Timestamp: "2024-06-14"},
		{Crc: 67890, KeySize: 4, ValueSize: 20, Key: "key5", Value: []byte("val5"), Tombstone: true, Timestamp: "2024-06-15"},
		{Crc: 12345, KeySize: 4, ValueSize: 500, Key: "key6", Value: []byte("val6PERSA PERSIC"), Tombstone: false, Timestamp: "2024-06-14"},
	}

	s.addElement("key1", &records[0])
	s.addElement("key2", &records[1])
	s.addElement("key3", &records[2])

	//key := "key8"
	key := "key3"

	node := s.searchElement(key)
	if node != nil {
		fmt.Printf("Found key: %s, value %s", node.key, string(node.value.Value))
		fmt.Println()
	} else {
		fmt.Printf("Key %s not found\n", key)
	}

	s.addElement("key3", &records[3])
	node = s.searchElement(key)
	if node != nil {
		fmt.Printf("Found key: %s, value %s", node.key, string(node.value.Value))
		fmt.Println()
	} else {
		fmt.Printf("Key %s not found\n", key)
	}
	s.removeElement("key3")

	node = s.searchElement("key3")
	fmt.Println(node)

	sortedRecords := s.sortElements()
	for _, r := range sortedRecords {
		fmt.Println(r)
	}
}
*/
