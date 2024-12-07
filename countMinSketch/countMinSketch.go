package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
)

type countMinSketch struct {
	tabela      [][]uint
	hesFunkcije []HashWithSeed
	k           uint
	m           uint
}

func napraviCountMinScetch(epsilon, delta float64) countMinSketch {
	k := CalculateK(delta)
	m := CalculateM(epsilon)
	hesFunkcije := CreateHashFunctions(k)
	tabela := make([][]uint, k)
	for i := range tabela {
		tabela[i] = make([]uint, m)
	}

	return countMinSketch{
		tabela:      tabela,
		hesFunkcije: hesFunkcije,
		k:           k,
		m:           m,
	}

}

func dodajElement(countMinSketch *countMinSketch, kljuc []byte) {
	hesVrijednosti := make([]uint64, countMinSketch.k)
	for i := range countMinSketch.hesFunkcije {
		hesVrijednosti[i] = countMinSketch.hesFunkcije[i].Hash(kljuc)
		j := hesVrijednosti[i] % uint64(countMinSketch.m)
		countMinSketch.tabela[i][j]++
	}

}

func nadjiUcestalost(countMinSketch *countMinSketch, kljuc []byte) uint {
	hesVrijednosti := make([]uint64, countMinSketch.k)
	odgovarajuceVrijednostiZaKljuc := make([]uint, countMinSketch.k)
	for i := range countMinSketch.hesFunkcije {
		hesVrijednosti[i] = countMinSketch.hesFunkcije[i].Hash(kljuc)
		j := hesVrijednosti[i] % uint64(countMinSketch.m)
		odgovarajuceVrijednostiZaKljuc[i] = countMinSketch.tabela[i][j]

	}

	najmanji := uint(999999)
	for i := range odgovarajuceVrijednostiZaKljuc {
		if uint(odgovarajuceVrijednostiZaKljuc[i]) < najmanji {
			najmanji = uint(odgovarajuceVrijednostiZaKljuc[i])
		}
	}

	return uint(najmanji)

}

func (cms *countMinSketch) serijalizuj(imeFajla string) {
	fajl, err := os.Create(imeFajla)
	if err != nil {
		log.Fatalf("Greska pri kreiranju fajla: %v", err)
	}
	defer fajl.Close()

	var bajtovi []byte
	kBajtovi := make([]byte, 4)
	mBajtovi := make([]byte, 4)
	binary.LittleEndian.PutUint32(kBajtovi, uint32(cms.k))
	binary.LittleEndian.PutUint32(mBajtovi, uint32(cms.m))
	bajtovi = append(bajtovi, kBajtovi...)
	bajtovi = append(bajtovi, mBajtovi...)

	for _, red := range cms.tabela {
		duzinaReda := make([]byte, 4)
		binary.LittleEndian.PutUint32(duzinaReda, uint32(len(red)))
		bajtovi = append(bajtovi, duzinaReda...)
		for _, vrijednost := range red {
			vrijednostBajtovi := make([]byte, 4)
			binary.LittleEndian.PutUint32(vrijednostBajtovi, uint32(vrijednost))
			bajtovi = append(bajtovi, vrijednostBajtovi...)
		}

	}

	brojFunkcija := make([]byte, 4)
	binary.LittleEndian.PutUint32(brojFunkcija, uint32(len(cms.hesFunkcije)))
	bajtovi = append(bajtovi, brojFunkcija...)
	for _, hesFunkcija := range cms.hesFunkcije {
		duzinaSeed := make([]byte, 4)
		binary.LittleEndian.PutUint32(duzinaSeed, uint32(len(hesFunkcija.Seed)))
		bajtovi = append(bajtovi, duzinaSeed...)
		bajtovi = append(bajtovi, hesFunkcija.Seed...)
	}

	_, err = fajl.Write(bajtovi)
	if err != nil {
		log.Fatalf("Greska pri upisivanju bajtova u fajl: %v", err)
	}

}

func (cms *countMinSketch) deserijalizuj(imeFajla string) {
	fajl, err := os.Open(imeFajla)
	if err != nil {
		log.Fatalf("Greska pri otvaranju fajla: %v", err)
	}

	defer fajl.Close()

	var bajtovi []byte

	podaciFajl, err := fajl.Stat()
	if err != nil {
		log.Fatalf("Greska pri citanju podataka o fajlu: %v", err)
	}

	velicinaFajla := podaciFajl.Size()
	bajtovi = make([]byte, velicinaFajla)
	_, err = fajl.Read(bajtovi)
	if err != nil {
		log.Fatalf("Greska pri citanju fajla: %v", err)
	}

	cms.k = uint(binary.LittleEndian.Uint32(bajtovi[:4]))
	cms.m = uint(binary.LittleEndian.Uint32(bajtovi[4:8]))
	bajtovi = bajtovi[8:]

	cms.tabela = make([][]uint, cms.k)
	for i := uint(0); i < cms.k; i++ {
		duzinaReda := binary.LittleEndian.Uint32(bajtovi[:4])
		bajtovi = bajtovi[4:]
		cms.tabela[i] = make([]uint, duzinaReda)
		for j := 0; j < int(duzinaReda); j++ {
			cms.tabela[i][j] = uint(binary.LittleEndian.Uint32(bajtovi[:4]))
			bajtovi = bajtovi[4:]
		}

	}

	brojFunkcija := binary.LittleEndian.Uint32(bajtovi[:4])
	bajtovi = bajtovi[4:]
	cms.hesFunkcije = make([]HashWithSeed, brojFunkcija)

	for i := 0; i < int(brojFunkcija); i++ {
		duzinaSeed := binary.LittleEndian.Uint32(bajtovi[:4])
		bajtovi = bajtovi[4:]
		cms.hesFunkcije[i].Seed = make([]byte, duzinaSeed)
		copy(cms.hesFunkcije[i].Seed, bajtovi[:duzinaSeed])
		bajtovi = bajtovi[duzinaSeed:]

	}

}
func main() {
	epsilon := 0.01
	delta := 0.01
	countMinSketch := napraviCountMinScetch(epsilon, delta)

	kljuc1 := []byte("jabuka")
	kljuc2 := []byte("banana")
	kljuc3 := []byte("narandza")
	kljuc4 := []byte("jabuka")

	dodajElement(&countMinSketch, kljuc1)
	dodajElement(&countMinSketch, kljuc2)
	dodajElement(&countMinSketch, kljuc3)
	dodajElement(&countMinSketch, kljuc4)

	fmt.Printf("Ucestalost jabuke je: %d\n", nadjiUcestalost(&countMinSketch, kljuc1))
	fmt.Printf("Ucestalost banane je: %d\n", nadjiUcestalost(&countMinSketch, kljuc2))
	fmt.Printf("Ucestalost narandze je: %d\n", nadjiUcestalost(&countMinSketch, kljuc3))

	imeFajla := "countMinSketch_podaci.bin"
	countMinSketch.serijalizuj(imeFajla)
	fmt.Println("Uspijesno sam serijalizovao u fajl")

	countMinSketch2 := countMinSketch

	countMinSketch2.deserijalizuj(imeFajla)
	fmt.Println("Uspijesno sam deserijalizovao iz fajla")

	fmt.Printf("Ucestalost jabuke (posle deserijalizacije) je: %d\n", nadjiUcestalost(&countMinSketch2, kljuc1))
	fmt.Printf("Ucestalost banane (posle deserijalizacije) je: %d\n", nadjiUcestalost(&countMinSketch2, kljuc2))
	fmt.Printf("Ucestalost narandze (posle deserijalizacije) je: %d\n", nadjiUcestalost(&countMinSketch2, kljuc3))

}
