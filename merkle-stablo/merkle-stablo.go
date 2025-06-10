package merkleStablo

import (
	"NASP-PROJEKAT/data"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log"
	"os"
)

// Struktura cvora u Merkle stablu gdje svaki cvor ima svoje lijevo i desno dijete i ssvoj hash

type MerkleNode struct {
	LeftChild  *MerkleNode
	RightChild *MerkleNode
	Hash       []byte
	Data       []byte
	NumLeaves  int32 // Broj listova ispod ovog cvora
}

// Struktura Merkle stabla gdje stablo ima svoj korijen

type MerkleTree struct {
	Root *MerkleNode
}

// Funckija koja hesira podatke, prima podatke kao niz bajtova

func Hash(data []byte) []byte {
	h := sha256.New()
	h.Write(data)
	return h.Sum(nil)
}

// Funkcija koja kreira listove stabla, prima niz blokova i vraca listove stabla

func CreateLeafNodes(blocks []*data.Block) []*MerkleNode {
	var leafNodes []*MerkleNode
	for _, block := range blocks {
		leafNodes = append(leafNodes, &MerkleNode{
			Hash:      Hash(block.Records),
			Data:      block.Records,
			NumLeaves: 1,
		})
	}
	return leafNodes
}

// funkcija koja formira Merkle stablo od dna ka vrhu, prima listove stabla i vraca korijen stabla, rekurzivno se poziva kako bi se formiralo stablo
// kombinuju se hesevi djece kako bi se kreirali roditelji

func BuildMerkleTreeBottomUp(nodes []*MerkleNode) *MerkleNode {
	if len(nodes) == 1 {
		return nodes[0]
	}

	var parentNodes []*MerkleNode
	for i := 0; i < len(nodes); i += 2 {
		var left *MerkleNode = nodes[i]
		var right *MerkleNode

		if i+1 < len(nodes) {
			right = nodes[i+1]
		} else {
			right = &MerkleNode{
				Hash:      Hash([]byte{}), // Ako nema desnog djeteta, koristimo prazan hash
				NumLeaves: 0,
			}
		}
		combinedHash := append(left.Hash, right.Hash...)
		parentNode := &MerkleNode{
			LeftChild:  left,
			RightChild: right,
			Hash:       Hash(combinedHash),
			NumLeaves:  left.NumLeaves + right.NumLeaves,
		}
		parentNodes = append(parentNodes, parentNode)
	}

	return BuildMerkleTreeBottomUp(parentNodes)
}

// Funkcija za serijalizaciju Merkle stabla, koristi se BFS za obilazak stabla, koristi se red za obilazak cvorova
// koristi se marker 0 za nil cvor i marker 1 za stvaran cvor
// funkcija prima korijen stabla i datoteku u koju se serijalizuje stablo

func DeserializeMerkleTree(file *os.File) (*MerkleNode, error) {
	var readNode func() (*MerkleNode, error)

	readNode = func() (*MerkleNode, error) {
		var marker uint32
		if err := binary.Read(file, binary.LittleEndian, &marker); err != nil {
			return nil, err
		}

		if marker == 0 {
			return nil, nil
		}

		var hashLength uint32
		if err := binary.Read(file, binary.LittleEndian, &hashLength); err != nil {
			return nil, err
		}
		hash := make([]byte, hashLength)
		if err := binary.Read(file, binary.LittleEndian, hash); err != nil {
			return nil, err
		}

		var NumLeaves uint32
		if err := binary.Read(file, binary.LittleEndian, &NumLeaves); err != nil {
			return nil, err
		}

		// provjerava se da li postoji data polje
		var dataMarker uint32
		if err := binary.Read(file, binary.LittleEndian, &dataMarker); err != nil {
			return nil, err
		}

		var dataBytes []byte
		if dataMarker == 1 {
			var dataLength uint32
			if err := binary.Read(file, binary.LittleEndian, &dataLength); err != nil {
				return nil, err
			}
			dataBytes = make([]byte, dataLength)
			if err := binary.Read(file, binary.LittleEndian, dataBytes); err != nil {
				return nil, err
			}
		}

		node := &MerkleNode{
			Hash:      hash,
			Data:      dataBytes,
			NumLeaves: int32(NumLeaves),
		}

		left, err := readNode()
		if err != nil {
			return nil, err
		}
		right, err := readNode()
		if err != nil {
			return nil, err
		}

		node.LeftChild = left
		node.RightChild = right

		return node, nil
	}

	root, err := readNode()
	if err != nil {
		return nil, err
	}
	return root, nil
}

// Funkcija koja poredi dva Merkle stabla i vraca indeks prvog lista koji se razlikuje
// prima dva root korijena dva stabla koji se porede
func CompareTrees(root1, root2 *MerkleNode) int32 {
	var compare func(node1, node2 *MerkleNode, currentLeafIndex int32) (int32, bool)

	compare = func(node1, node2 *MerkleNode, currentLeafIndex int32) (int32, bool) {
		// 1. Slucaj: Oba cvora su nil, identicni su
		if node1 == nil && node2 == nil {
			return -1, false // -1 pokazuje da nema razlike, false znaci da nije pronadjena razlika na ovom nivou
		}

		// 2. Slucaj: Jedan cvor je nil, a drugi nije
		if node1 == nil || node2 == nil {
			// ako je jedno stablo krace, razlika je na indeksu prvog lista koji nedostaje
			return currentLeafIndex, true // currentLeafIndex je vec na pravom mjestu
		}

		// 3. Slucaj: Hesevi se podudaraju (podstablo je identicno, to znaci da se preskace)
		if bytes.Equal(node1.Hash, node2.Hash) {
			// pomice currentLeafIndex za broj listova u ovom podstablu
			// ne vraca se indeks razlike, vec samo novi currentLeafIndex za dalju potragu
			return currentLeafIndex + node1.NumLeaves, false // false = nije pronadjena razlika na ovom nivou
		}

		// 4. Slucaj: Hesevi se ne podudaraju, ali se trenutno ne nalazimo na listu (idemo u dubinu stabla)
		if node1.LeftChild != nil || node1.RightChild != nil { // provjerava se da li je da li je node1 (ili node2) interni cvor
			// provjerava se lijevo podstablo
			if idx, found := compare(node1.LeftChild, node2.LeftChild, currentLeafIndex); found {
				return idx, true // razlika je pronadjena u lijevom podstablu, odmah funckija vraca index i true
			} else if idx != -1 { // Ako lijeva grana nije imala razliku, ali je vratila novi indeks (doslo je do preskakanja), azurira se currentLeafIndex
				currentLeafIndex = idx
			}

			// provjeraa se desno podstablo
			if idx, found := compare(node1.RightChild, node2.RightChild, currentLeafIndex); found {
				return idx, true // razlika je pronadjena u desnom podstablu, odmah se vraca index i true
			} else if idx != -1 { // Ako desna grana nije imala razliku, ali je vratila novi indeks (doslo je do preskakanja), azurira se currentLeafIndex
				currentLeafIndex = idx
			}

			return -1, false // nijedna razlika nije pronadjena u podstablu
		}

		// 5. Slucaj: Hesevi se ne podudaraju, i imamo dva lista (direktno uporedjujem podatke)
		// dosao sam do listova i hesevi se razlikuju
		if !bytes.Equal(node1.Data, node2.Data) {
			return currentLeafIndex, true // pronasao sam razliku na ovom listu
		}

		// Ako su listovi identicni (što se ne bi trebalo desiti ovde ako je hash bio razlicit)
		// Ili ako je samo jedan list (nedostaje drugi) - to je vec pokriveno gore.
		return currentLeafIndex + 1, false // pomjera se indeks za 1, nije pronadjena razlika na ovom listu
	}

	diffIndex, found := compare(root1, root2, 0)
	if found { // Ako je found == true, znaci da je razlika zaista pronadjena
		return diffIndex
	}
	// Ako found != true, a diffIndex == -1, znaci da nema razlike
	// Ako found != true, a diffIndex != -1, znaci da smo samo preskocili sve listove bez pronalaska razlike
	return -1 // vraca -1 ako nije pronadjena razlika
}

// Funkcija za serijalizaciju Merkle stabla, koristi se BFS za obilazak stabla, koristi se red za obilazak cvorova
// koristi se marker 0 za nil cvor i marker 1 za stvaran cvor
// funkcija prima korijen stabla i datoteku u koju se serijalizuje stablo, vraca gresku dodje do nje

func SerializeMerkleTree(root *MerkleNode, file *os.File) error {
	var writeNode func(node *MerkleNode) error

	writeNode = func(node *MerkleNode) error {
		if node == nil {
			return binary.Write(file, binary.LittleEndian, uint32(0))
		}

		// Marker da čvor postoji
		if err := binary.Write(file, binary.LittleEndian, uint32(1)); err != nil {
			return err
		}

		// Pišemo hash
		if err := binary.Write(file, binary.LittleEndian, uint32(len(node.Hash))); err != nil {
			return err
		}
		if err := binary.Write(file, binary.LittleEndian, node.Hash); err != nil {
			return err
		}

		if err := binary.Write(file, binary.LittleEndian, uint32(node.NumLeaves)); err != nil {
			return err
		}

		// Pišemo data (ako postoji)
		if node.Data != nil {
			if err := binary.Write(file, binary.LittleEndian, uint32(1)); err != nil {
				return err
			}
			if err := binary.Write(file, binary.LittleEndian, uint32(len(node.Data))); err != nil {
				return err
			}
			if err := binary.Write(file, binary.LittleEndian, node.Data); err != nil {
				return err
			}
		} else {
			if err := binary.Write(file, binary.LittleEndian, uint32(0)); err != nil {
				return err
			}
		}

		// Rekurzivno pišemo lijevo i desno dijete
		if err := writeNode(node.LeftChild); err != nil {
			return err
		}
		if err := writeNode(node.RightChild); err != nil {
			return err
		}

		return nil
	}

	return writeNode(root)
}

func main() {
	// 1. Kreiramo testne blokove
	blocks := []*data.Block{
		{Records: []byte("Zapis 1")},
		{Records: []byte("Zapis 2")},
		{Records: []byte("Zapis 3")},
		{Records: []byte("Zapis 4")},
	}

	// 2. Kreiramo listove i izgradimo Merkle stablo
	leafNodes := CreateLeafNodes(blocks)
	root := BuildMerkleTreeBottomUp(leafNodes)

	// 3. Otvaramo fajl za serijalizaciju
	fileName := "merkle_tree_test.dat"
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("Greska pri kreiranju fajla: %v", err)
	}
	defer file.Close()

	// 4. Serijalizujemo stablo
	err = SerializeMerkleTree(root, file)
	if err != nil {
		log.Fatalf("Greska pri serijalizaciji: %v", err)
	}
	fmt.Println("Merkle stablo je uspjesno serijalizovano.")

	// 5. Ponovo otvaramo fajl za čitanje
	fileForRead, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("Greska pri otvaranju fajla za čitanje: %v", err)
	}
	defer fileForRead.Close()

	// 6. Deserijalizujemo stablo
	deserializedRoot, err := DeserializeMerkleTree(fileForRead)
	if err != nil {
		log.Fatalf("Greška pri deserijalizaciji: %v", err)
	}
	fmt.Println("Merkle stablo je uspjesno deserijalizovano.")

	// 7. Poređenje originalnog i deserijalizovanog stabla
	fmt.Println("Poređenje originalnog i deserijalizovanog stabla:")
	diffIndex := CompareTrees(root, deserializedRoot)
	if diffIndex == -1 {
		fmt.Println("Stabla su identicna.")
	} else {
		fmt.Printf("Stabla se razlikuju na listu sa indeksom: %d\n", diffIndex)
	}
}
