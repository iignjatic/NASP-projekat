package bloomfilter

import "fmt"

type BloomFilter struct {
	bitset    []bool
	hashFuncs []HashWithSeed
	k         uint
	m         uint
}

// inicijalizuje bloom filter sa ocekivanim brojem elemenata i falsePositive procentom
func createBloomFilter(expectedElements int, falsePositiveRate float64) *BloomFilter {
	m := CalculateM(expectedElements, falsePositiveRate)
	k := CalculateK(expectedElements, m)
	hashFunctions := CreateHashFunctions(uint32(k))
	return &BloomFilter{
		bitset:    make([]bool, m),
		hashFuncs: hashFunctions,
		k:         k,
		m:         m,
	}
}

// dodaje novi element u bloom filter sa string kljucem
func (bf *BloomFilter) addElement(element string) {
	for i := 0; i < int(bf.k); i++ {
		index := bf.hashFuncs[i].Hash([]byte(element)) % uint64(bf.m)
		bf.bitset[index] = true
	}
}

// provjerava da li je element mozda tu, a ako vrati false znaci da element sigurno nije tu
func (bf *BloomFilter) check(element string) bool {
	for i := 0; i < int(bf.k); i++ {
		index := bf.hashFuncs[i].Hash([]byte(element)) % uint64(bf.m)
		if !bf.bitset[index] {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) delete() {
	bf.bitset = nil
	bf.hashFuncs = nil
	bf.k = 0
	bf.m = 0
}

func main() {
	bf := createBloomFilter(1000, 0.01)
	bf.addElement("jabuka")
	bf.addElement("banana")
	bf.addElement("kruska")

	fmt.Println(bf.check("jabuka"))
	fmt.Println(bf.check("salata"))

	bf.delete()
	if bf.bitset == nil && bf.hashFuncs == nil && bf.k == 0 && bf.m == 0 {
		fmt.Println("Fajl je obrisan")
	}
}
