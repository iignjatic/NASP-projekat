package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
)

// Blok struktura koja je tu samo privremeno, dobavljace se blokovi iz SStabla

type Block struct {
	records []byte
}

// Struktura cvora u Merkle stablu gdje svaki cvor ima svoje lijevo i desno dijete i ssvoj hash

type MerkleNode struct {
	LeftChild  *MerkleNode
	RightChild *MerkleNode
	Hash       []byte
}

// Struktura Merkle stabla gdje stablo ima svoj korijen

type MerkleTree struct {
	Root *MerkleNode
}

// Funckija koja hesira podatke, prima podatke kao niz bajtova

func hash(data []byte) []byte {
	h := sha256.New()
	h.Write(data)
	return h.Sum(nil)
}

// Funkcija koja kreira listove stabla, prima niz blokova i vraca listove stabla

func createLeafNodes(blocks []*Block) []*MerkleNode {
	var leafNodes []*MerkleNode
	for _, block := range blocks {
		leafNodes = append(leafNodes, &MerkleNode{
			Hash: hash(block.records),
		})
	}
	return leafNodes
}

// funkcija koja formira Merkle stablo od dna ka vrhu, prima listove stabla i vraca korijen stabla, rekurzivno se poziva kako bi se formiralo stablo
// kombinuju se hesevi djece kako bi se kreirali roditelji

func buildMerkleTreeBottomUp(nodes []*MerkleNode) *MerkleNode {
	if len(nodes) == 1 {
		return nodes[0]
	}

	var parentNodes []*MerkleNode
	for i := 0; i < len(nodes); i += 2 {
		if i+1 < len(nodes) {
			combinedHash := append(nodes[i].Hash, nodes[i+1].Hash...)
			parentNodes = append(parentNodes, &MerkleNode{
				LeftChild:  nodes[i],
				RightChild: nodes[i+1],
				Hash:       hash(combinedHash),
			})
		} else {
			emptyNode := &MerkleNode{
				Hash: hash([]byte{}),
			}
			combinedHash := append(nodes[i].Hash, emptyNode.Hash...)
			parentNodes = append(parentNodes, &MerkleNode{
				LeftChild:  nodes[i],
				RightChild: emptyNode,
				Hash:       hash(combinedHash),
			})
		}
	}
	return buildMerkleTreeBottomUp(parentNodes)
}

// Funkcija koja uporedjuje dva Merkle stabla, rekurzivno se poziva kako bi se uporedili svi cvorovi stabla, prima cvorove originalno i izmjenjenog stabla
// vraca da li su stabla identicna ili ne
func compareMerkleTrees(originalNode, currentNode *MerkleNode) bool {
	if originalNode == nil && currentNode == nil {
		return true
	}

	if originalNode == nil || currentNode == nil {
		return false
	}

	if !bytes.Equal(originalNode.Hash, currentNode.Hash) {
		return false
	}

	return compareMerkleTrees(originalNode.LeftChild, currentNode.LeftChild) && compareMerkleTrees(originalNode.RightChild, currentNode.RightChild)
}

// ispitujemo da li su stabla identicna ili ne, pozivamo funkciju za uporedjivanje stabala

func validateMerkleTree(originalRoot *MerkleNode, modifiedRoot *MerkleNode) {
	if compareMerkleTrees(originalRoot, modifiedRoot) {
		fmt.Println("Stablo je identicno.")
	} else {
		fmt.Println("Stablo nije identicno.")
	}

}

// Funkcija za serijalizaciju Merkle stabla, koristi se BFS za obilazak stabla, koristi se red za obilazak cvorova
// koristi se marker 0 za nil cvor i marker 1 za stvaran cvor
// funkcija prima korijen stabla i datoteku u koju se serijalizuje stablo, vraca gresku dodje do nje

func serializeMerkleTree(root *MerkleNode, file *os.File) error {
	queue := []*MerkleNode{root}
	for len(queue) > 0 {
		currentNode := queue[0]
		queue = queue[1:]

		if currentNode == nil {
			// Serijalizujemo nil cvor kao marker 0
			if err := binary.Write(file, binary.LittleEndian, uint32(0)); err != nil {
				return err
			}
			continue
		}

		// Serijalizuje se stvaran cvor (marker 1)
		if err := binary.Write(file, binary.LittleEndian, uint32(1)); err != nil {
			return err
		}

		// Serijalizacija hash-a trenutnog cvora
		if err := binary.Write(file, binary.LittleEndian, uint32(len(currentNode.Hash))); err != nil {
			return err
		}
		if err := binary.Write(file, binary.LittleEndian, currentNode.Hash); err != nil {
			return err
		}

		// Dodavanje lijeve i desne grane u red za serijalizaciju
		queue = append(queue, currentNode.LeftChild, currentNode.RightChild)
	}
	return nil
}

// Funkcija za deserijalizaciju Merkle stabla, prima datoteku u kojoj je serijalizovano stablo i vraca korijen stabla
func deserializeMerkleTree(file *os.File) (*MerkleNode, error) {
	var readNode func() (*MerkleNode, error)

	// Funkcija za citanje jednog čvora
	readNode = func() (*MerkleNode, error) {
		var marker uint32
		if err := binary.Read(file, binary.LittleEndian, &marker); err != nil {
			return nil, err
		}

		// Ako je marker 0, to znaci da je cvor nil
		if marker == 0 {
			return nil, nil
		}

		// Ako je marker 1, znaci da citamo stvaran cvor
		var hashLength uint32
		if err := binary.Read(file, binary.LittleEndian, &hashLength); err != nil {
			return nil, err
		}

		hash := make([]byte, hashLength)
		if err := binary.Read(file, binary.LittleEndian, &hash); err != nil {
			return nil, err
		}

		// Kreira se novi MerkleNode sa procitanim hash-om
		node := &MerkleNode{Hash: hash}

		// Rekurzivno se cita lijeva i desna grana stabla
		leftChild, err := readNode()
		if err != nil {
			return nil, err
		}
		rightChild, err := readNode()
		if err != nil {
			return nil, err
		}

		// Povezuje se cvor sa njegovim granama
		node.LeftChild = leftChild
		node.RightChild = rightChild

		return node, nil
	}

	// Ciga se korijen stabla
	root, err := readNode()
	if err != nil {
		return nil, err
	}
	return root, nil
}

func main() {
	// Pravim testne blokove
	block1 := &Block{records: []byte("block1_data")}
	block2 := &Block{records: []byte("block2_data")}
	block3 := &Block{records: []byte("block3_data")}
	block4 := &Block{records: []byte("block4_data")}
	blocks := []*Block{block1, block2, block3, block4}

	// pravim listove stabla
	leafNodes := createLeafNodes(blocks)

	//  Pravi se originalno Merkle stablo
	originalRoot := buildMerkleTreeBottomUp(leafNodes)

	// Kreiranje MerkleTree objekta
	originalTree := &MerkleTree{Root: originalRoot}
	fmt.Printf("Korjen hash-a: %x\n", originalTree.Root.Hash)

	// Serijalizacija u privremenu datoteku
	tempFile, err := os.CreateTemp("", "merkle_tree")
	if err != nil {
		fmt.Printf("Greska pri kreiranju privremene datoteke: %v\n", err)
		return
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	// Serijalizacija stabla
	err = serializeMerkleTree(originalTree.Root, tempFile)
	if err != nil {
		fmt.Printf("Greska kod serijalizacije: %v\n", err)
		return
	}

	// Resetovanje kursora na pocetak datoteke za citanje
	tempFile.Seek(0, 0)

	// Deserijalizacija stabla
	deserializedRoot, err := deserializeMerkleTree(tempFile)
	if err != nil {
		fmt.Printf("Greska kod deserijalizacije: %v\n", err)
		return
	}

	// Ispis haseva originalnog i deserijalizovanog korena
	fmt.Printf("Originalni korijen hesa: %x\n", originalTree.Root.Hash)
	fmt.Printf("Deserijalizovani korijen hesa: %x\n", deserializedRoot.Hash)

	// Uporedjujem heseva originalnog i deserijalizovanog korena stabla
	if bytes.Equal(originalTree.Root.Hash, deserializedRoot.Hash) {
		fmt.Println("Hasevi originalnog i deserijalizovanog korena su identicni.")
	} else {
		fmt.Println("Hesevi originalnog i deserijalizovanog korjena nisu identicni.")
	}

	// Mijenjam jedan blok jer hocu da provjrim validaciju stabla
	block2 = &Block{records: []byte("modified_block2_data")}
	blocks[1] = block2

	// Pravim listove za novo Merkle stablo
	leafNodes = createLeafNodes(blocks)

	// Pravim novo Merkle stablo
	updatedRoot := buildMerkleTreeBottomUp(leafNodes)
	updatedTree := &MerkleTree{Root: updatedRoot}
	fmt.Printf("Modifikovan kojren hesa: %x\n", updatedTree.Root.Hash)

	validateMerkleTree(originalRoot, updatedRoot)

	// Provjera da li je stablo identicno sa samim sobom
	validateMerkleTree(originalRoot, originalRoot)

}
