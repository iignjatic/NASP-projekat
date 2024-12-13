package main

import (
	"fmt"

	"golang.org/x/text/search"
)

//minimalan broj djece je polovina kljuceva a maksimalan

//svaki put kad dodam kljuc, dodaje se u korjen u sortiranom redoslijedu
//kad se napuni korjen, uzimam dva najmanja kljuca i od njih pravim jedan node , uzimam dva najveca kljuca i od njih pravim
//drugi node, sredisnji clan postaje korjenski node koji pokazuje na lijevo i desno dijete
//dalje u dodavanju korjen odlucuje koji node sledeci da posjetim i dodaje se u odgovarajuci node u sortiranom redoslijedu
//kad se popuni splituje se a srednji node se prebacuje u roditelja

//prilikom brisanja LISTA kad se obrise element i time je broj clanova manji od minimuma, uzimam najveci s lijeve strane
//ili najmanji sa desne strane i dodajem u roditelja, a clan seperator iz roditelja spustam u node iz kog sam brisala
//ali kad je i sibling node na minimumu , onda spajam trenutni node, sibling i separator izmedju njih iz korjena
//prilikom brisanja noda koji nije list dodajemo mu najveci clan iz lijevog podstabla ili najmanji iz desnog

//broj djece je ogranicen izmedju t i 2t , gdje je t red stabla
//Broj ključeva je ograničen između t−1 i 2t−1, gdje je t red stabla.
type Node struct{
    keys []string
    children []*Node
    isLeaf bool
    t int   //minimalan stepen stabla

}//stepen stabla se bira u skladu sa kes memorijom, da bi se ucitao cijeli blok??

type BTree struct{
    root *Node
    t int    //minimalan stepen stabla
}
func(node *Node) search(y string) (*Node, int){  //prosledjujem koren stabla, od njega krecem pretragu
    i:= 0
    if (len(node.keys) == 0){ //ako je stablo prazno
        return nil, -1
    }
    for i < len(node.keys){ //idem kroz sve kljuceve 
        if y > node.keys[i]{
            i ++;              //krecem se kroz kljuceve sve dok je trazena vrijednost veca od njih
        }else if y == node.keys[i]{
            return node,i  
        }else{
            //spustam se u podstablo tog kljuca od koga je vrijednost manja
            return node.children[i].search(y)
        }
            
        
    }
    return nil, -1  //nije nadjen element u stablu
}
func(tree *BTree) insert (y string, t int) (*Node){  //dodavanje elementa u stablo
    newNode:= Node(keys: []string, children: []*Node, isLeaf: true, t: t)  //prvo napravim node sa prosledjenom vrijednoscu
    s:= tree.root
    //dodavanje pocinje neuspjesnom pretragom
    position = newNode.search(y)  //dodjem do noda u koji trebam da dodam element
    if (position == nil){
        return nil  //
    }
}

func(tree *BTree) delete(y string)(*Node){  

}