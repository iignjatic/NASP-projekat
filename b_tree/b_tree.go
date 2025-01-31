package b_tree

import (
	"NASP-PROJEKAT/data"
	"errors"
	"fmt"
	"time"
)

// m is order of BTree
// max m children for every node
// min (m/2) upper children for every node
// max m-1 keys for every node
// min (m/2) upper - 1 keys for every node

type BTreeNode struct {
	records     []*data.Record
	children    []*BTreeNode
	recordNum   int
	childrenNum int
}

type BTree struct {
	root *BTreeNode
	m    int //order
}

func NewBTreeNode(order int) *BTreeNode {
	return &BTreeNode{
		records:     make([]*data.Record, 0, order-1),
		children:    make([]*BTreeNode, 0, order),
		recordNum:   0,
		childrenNum: 0,
	}
}

// funkcija koja provjerava da li je cvor list
func (node *BTreeNode) isLeaf() bool {
	return len(node.children) == 0
}

func NewBTree(order int) *BTree {
	return &BTree{
		root: NewBTreeNode(order),
		m:    order,
	}
}

// funkcija koja ispisuje stablo
func (tree *BTree) PrintTree(node *BTreeNode, level int) {
	if node == nil {
		return
	}
	// uvalcenje za svaki nivo
	indent := ""
	for i := 0; i < level; i++ {
		indent += "  "
	}

	fmt.Printf("%sČvor na nivou %d:\n", indent, level)

	// ispis svih records u trenutnom čvoru
	for _, record := range node.records {
		//fmt.Printf("%s  Record %d: Key='%s', Value='%s', Tombstone=%v, Timestamp=%s\n",
		//	indent, i+1, record.Key, string(record.Value), record.Tombstone, record.Timestamp)
		fmt.Printf("%s Key='%s'", indent, record.Key)
	}
	fmt.Println()
	// rekurzivno ispisi djecu cvora
	for _, child := range node.children {
		tree.PrintTree(child, level+1)
	}
}

// binarna pretraga kljuca unutar jednog covora
func (node *BTreeNode) search(key string) (int, bool) {
	low, high := 0, node.recordNum
	var mid int
	for low < high {
		mid = (low + high) / 2
		if key > node.records[mid].Key {
			low = mid + 1
		} else if key < node.records[mid].Key {
			high = mid
		} else {
			return mid, true
		}
	}
	return low, false
}

// pretraga koja se bazira na binarnoj pretrazi
// ulazni parametar: kljuc koji se trazi
// povratne vrijednosti: ako je pronasao record vraca njega i nil za gresku
func (t *BTree) Get(key string) (*data.Record, error) {

	for next := t.root; next != nil; {
		index, found := next.search(key)

		if found {
			return next.records[index], nil
		}

		if len(next.children) == 0 {
			return nil, errors.New("key not found")
		}

		next = next.children[index]
	}

	return nil, errors.New("key not found")
}

// funkcija koja sluzi za logicko brisanje recorda na osnovu kljuca
func (tree *BTree) Delete(key string) error {
	record, err := tree.Get(key)
	if err != nil {
		return err
	}
	record.Tombstone = true
	record.Timestamp = time.Now().UTC().Format(time.RFC3339)
	return nil
}

// dodaje novi record na odredjeni index u cvoru
func (node *BTreeNode) insertRecordAt(index int, record *data.Record) {
	if index < len(node.records) {
		// kopira se na mjesta od index+1 do recordNum+1 vrijednost od index do recordNum
		// odnosno pravi se jedno prazno mjesto ako ne dodajemo na kraj
		node.records = append(node.records, nil)
		copy(node.records[index+1:node.recordNum+1], node.records[index:node.recordNum])
		node.records[index] = record
	} else {
		node.records = append(node.records, record)
	}
	node.recordNum++
}

// dodaje novo dijete cvoru na odredjenom index-u
func (node *BTreeNode) insertChildAt(index int, childNode *BTreeNode) {
	if index < len(node.children) {
		node.children = append(node.children, nil)
		copy(node.children[index+1:], node.children[index:])
		node.children[index] = childNode
	} else {
		node.children = append(node.children, childNode)
	}
	node.childrenNum++
}

// funkcija koja split-uje odredjeni cvor
// u proslijedjenom cvoru ostaju elementi od pocetka do sredine (ne ukljucujuci srednji)
// povratne vrijednosti su: sredisnji record (koji se kasnije podize nivo iznad)
// i novi node u kojem se nalaze record-i od sredisnjeg do kraja sa odgovarajucom djecom
func (tree *BTree) split(node *BTreeNode) (*data.Record, *BTreeNode) {
	minRecords := tree.m/2 - 1
	mid := minRecords
	midRecord := node.records[mid]

	newNode := NewBTreeNode(tree.m)
	newNode.records = append(newNode.records, node.records[mid+1:]...)
	newNode.recordNum = len(newNode.records)

	if !node.isLeaf() {
		newNode.children = append(newNode.children, node.children[mid+1:]...)
		newNode.childrenNum = len(newNode.children)
	}

	node.records = node.records[:mid]
	node.recordNum = len(node.records)

	if !node.isLeaf() {
		node.children = node.children[:mid+1]
		node.childrenNum = len(node.children)
	}

	return midRecord, newNode
}

// funkcija koja provjerava da li postoji lijevi ili desni sibling koji nije popunjen
// ako pronadje jedan od njih dodaje novi record na odgovarajucu poziciju iz roditelja
// record iz cvora koji je bio prepun prebacuje u roditelja i azurira cvor koji je bio prepun
func (tree *BTree) rotate(node *BTreeNode, index int) bool {
	// provjerava da li postoji lijevi sibling
	if index > 0 {
		leftSibling := node.children[index-1]
		if len(leftSibling.records) < tree.m-1 {
			leftSibling.insertRecordAt(len(leftSibling.records), node.records[index-1])
			if !node.isLeaf() && len(node.children[index].children) > 0 {
				leftSibling.insertChildAt(len(leftSibling.children), node.children[index].children[0])
				node.children[index].children = node.children[index].children[1:]
				node.children[index].childrenNum--
			}
			// prebaci kljuc iz trenutnog cvora u roditelja
			node.records[index-1] = node.children[index].records[0]
			node.children[index].records = node.children[index].records[1:]
			node.children[index].recordNum--
			return true
		}
	}

	//provjerava da li postoji desni sibling
	if index < len(node.children)-1 {
		rightSibling := node.children[index+1]
		if len(rightSibling.records) < tree.m-1 {
			// rotiraj iz roditelja u desni sibling
			rightSibling.insertRecordAt(0, node.records[index])
			if !node.isLeaf() && len(node.children[index].children) > 0 {
				rightSibling.insertChildAt(0, node.children[index].children[len(node.children[index].children)-1])
				node.children[index].children = node.children[index].children[:len(node.children[index].children)-1]
				node.children[index].childrenNum--
			}
			// prebaci kljuc iz trenutnog cvora u roditelja
			node.records[index] = node.children[index].records[len(node.children[index].records)-1]
			node.children[index].records = node.children[index].records[:len(node.children[index].records)-1]
			node.children[index].recordNum--
			return true
		}
	}
	return false
}

// pomocna funkcija za insert
// provjerava prvo da li kljuc postoji, ako postoji samo azurira vrijedost
// ako je cvor do kojeg smo stigli list, znaci da smo dosli do mjesta na kom treba dodati record
// ako nije list nastavlja se pozivati rekurzivno metoda nad odgovarajucim djetetom
func (tree *BTree) insert(node *BTreeNode, record *data.Record) bool {
	index, found := node.search(record.Key)
	var inserted bool
	if found {
		node.records[index] = record
		return false
	}

	if node.isLeaf() {
		node.insertRecordAt(index, record)
		inserted = true
	} else {
		inserted = tree.insert(node.children[index], record)

		if len(node.children[index].records) > tree.m-1 {
			if !tree.rotate(node, index) {
				midRecord, newNode := tree.split(node.children[index])
				node.insertRecordAt(index, midRecord)
				node.insertChildAt(index+1, newNode)
			}
		}
	}

	if node == tree.root && len(node.records) > tree.m-1 {
		tree.splitRoot()
	}

	return inserted
}

// funkcija radi podjelu korijena stabla, kada je to potrebno i kada je korijen prepunjen
func (tree *BTree) splitRoot() {
	newRoot := NewBTreeNode(tree.m)
	midRecord, newNode := tree.split(tree.root)
	newRoot.insertRecordAt(0, midRecord)
	newRoot.insertChildAt(0, tree.root)
	newRoot.insertChildAt(1, newNode)
	tree.root = newRoot
}

func (tree *BTree) Insert(key string, value []byte) {
	record := &data.Record{
		Key:       key,
		Value:     value,
		KeySize:   uint64(len(key)),
		ValueSize: uint64(len(value)),
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Tombstone: false,
	}

	if tree.root == nil {
		tree.root = NewBTreeNode(tree.m)
	}

	tree.insert(tree.root, record)
}

func (tree *BTree) InsertRecord(record *data.Record) {
	if tree.root == nil {
		tree.root = NewBTreeNode(tree.m)
	}

	tree.insert(tree.root, record)
}

func (tree *BTree) InOrderTraversal(node *BTreeNode, records *[]*data.Record) {
	if node == nil {
		return
	}

	for i := 0; i < node.recordNum; i++ {
		if i < len(node.children) {
			tree.InOrderTraversal(node.children[i], records)
		}

		//fmt.Printf("Key='%s', Value='%s'\n", node.records[i].Key, string(node.records[i].Value))
		*records = append(*records, node.records[i])
	}

	if len(node.children) > node.recordNum {
		tree.InOrderTraversal(node.children[node.recordNum], records)
	}
}

func (tree *BTree) GetSortedRecords() []*data.Record {
	var records []*data.Record
	tree.InOrderTraversal(tree.root, &records)
	return records
}

/*
func main() {
	// kreiramo B stablo sa redom 4
	tree := NewBTree(4)

	// dodajemo elemente
	//tree.Insert("a", []byte("value1"))
	//tree.Insert("b", []byte("value2"))
	//tree.Insert("c", []byte("value3"))
	//tree.Insert("d", []byte("value4"))
	//tree.Insert("e", []byte("value5"))
	//tree.Insert("f", []byte("value6"))
	//tree.Insert("g", []byte("value7"))
	//tree.Insert("h", []byte("value8"))
	//tree.Insert("i", []byte("value9"))
	//tree.Insert("j", []byte("value10"))
	//tree.Insert("k", []byte("value10"))
	//tree.Insert("l", []byte("value10"))
	//tree.Insert("m", []byte("value10"))
	//tree.Insert("n", []byte("value10"))

	tree.Insert("n", []byte("value1"))
	tree.Insert("m", []byte("value2"))
	tree.Insert("l", []byte("value3"))
	tree.Insert("k", []byte("value4"))
	tree.Insert("j", []byte("value5"))
	tree.Insert("i", []byte("value6"))
	tree.Insert("h", []byte("value7"))
	tree.Insert("g", []byte("value8"))
	tree.Insert("f", []byte("value9"))
	tree.Insert("e", []byte("value10"))
	tree.Insert("d", []byte("value10"))
	tree.Insert("c", []byte("value10"))
	tree.Insert("b", []byte("value10"))
	tree.Insert("a", []byte("value10"))
	record1 := data.Record{
		Key:   "A",
		Value: []byte("value 10"),
	}
	record2 := data.Record{
		Key:   "1",
		Value: []byte("value10"),
	}
	//tree.Insert("A", []byte("value10"))
	//tree.Insert("1", []byte("value10"))
	tree.InsertRecord(&record1)
	tree.InsertRecord(&record2)

	// ispisujemo stablo
	fmt.Println("Stablo nakon umetanja:")
	tree.PrintTree(tree.root, 0)

	// pretrazujemo kljuc
	key := "c"
	record, err := tree.Get(key)
	if err == nil {
		fmt.Printf("Pronadjen zapis: kljuc=%s, vrijednost=%s\n", record.Key, string(record.Value))
	} else {
		fmt.Printf("Kljuc %s nije pronadjen\n", key)
	}

	// brisemo kljuc
	err = tree.Delete("c")
	if err == nil {
		fmt.Printf("kljuc %s je obiljezen kao obrisan\n", key)
	} else {
		fmt.Printf("greska pri brisanju kljusa %s: %v\n", key, err)
	}

	// provjerava da li je kljuc "obrisan" (logicki)
	record, err = tree.Get("c")
	if err == nil {
		fmt.Printf("pronadjen zapis: kljuc=%s, vrijednost=%s, obrisan=%v\n", record.Key, string(record.Value), record.Tombstone)
	} else {
		fmt.Printf("Kljuc %s nije pronadjen nakon brisanja\n", key)
	}

	// ponovo ispis stabla
	fmt.Println("Stablo nakon brisanja:")
	tree.PrintTree(tree.root, 0)

	fmt.Println("InOrder obilazak:")
	tree.InOrderTraversal(tree.root)
}
*/
