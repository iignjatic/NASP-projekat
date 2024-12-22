package bloomfilter

import (
	"encoding/binary"
	"io"
	"os"
)

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

// brisanje bloomfiltera
func (bf *BloomFilter) delete() {
	bf.bitset = nil
	bf.hashFuncs = nil
	bf.k = 0
	bf.m = 0
}

func (bf *BloomFilter) serialize(filename string) error {
	// prvi nacin rad sa bufferom
	/*file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	buffer := []byte{}

	kbytes := make([]byte, 8) // uint64 zauzima 8 bajtova
	binary.LittleEndian.PutUint64(kbytes, uint64(bf.k))

	mbytes := make([]byte, 8) // uint64 zauzima 8 bajtova
	binary.LittleEndian.PutUint64(mbytes, uint64(bf.m))

	buffer = append(buffer, kbytes...)
	buffer = append(buffer, mbytes...)

	for _, bit := range bf.bitset {
		var b uint8
		if bit {
			b = 1
		} else {
			b = 0
		}
		buffer = append(buffer, b)
	}

	for _, hashFunc := range bf.hashFuncs {
		buffer = append(buffer, hashFunc.Seed...)
	}

	_, err = file.Write(buffer)
	if err != nil {
		return err
	}

	return nil*/

	// drugi nacin upisivanje svakog dijela posebno u fajl
	/*
		file, err := os.Create(filename)
		if err != nil {
			return err
		}
		defer file.Close()

		if err := binary.Write(file, binary.LittleEndian, uint64(bf.k)); err != nil {
			return err
		}

		if err := binary.Write(file, binary.LittleEndian, uint64(bf.m)); err != nil {
			return err
		}

		for _, bit := range bf.bitset {
			var b uint8
			if bit {
				b = 1
			} else {
				b = 0
			}
			if err := binary.Write(file, binary.LittleEndian, b); err != nil {
				return err
			}
		}

		for _, hashFunc := range bf.hashFuncs {
			    if _, err := file.Write(hashFunc.Seed); err != nil {
					return err
				}
		}

		return nil*/

	// treci nacin, kombinovan pristup buffer i direktno upisivanje
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := binary.Write(file, binary.LittleEndian, uint64(bf.k)); err != nil {
		return err
	}

	if err := binary.Write(file, binary.LittleEndian, uint64(bf.m)); err != nil {
		return err
	}

	const bufferSize = 4096 // 4 KB buffer
	buffer := make([]byte, 0, bufferSize)
	var currentByte byte
	bitCount := 0
	for _, bit := range bf.bitset {
		if bit {
			currentByte = currentByte | (1 << bitCount)
		}
		bitCount++

		if bitCount == 8 {
			buffer = append(buffer, currentByte)
			currentByte = 0
			bitCount = 0
		}

		if len(buffer) >= bufferSize {
			_, err = file.Write(buffer)
			if err != nil {
				return err
			}
			buffer = buffer[:0] // resetuje buffer
		}
	}
	if bitCount > 0 {
		buffer = append(buffer, currentByte)
	}
	if len(buffer) > 0 {
		_, err = file.Write(buffer)
		if err != nil {
			return err
		}
	}

	for _, hashFunc := range bf.hashFuncs {
		if _, err := file.Write(hashFunc.Seed); err != nil {
			return err
		}
	}

	return nil
}

func (bf *BloomFilter) deserialize(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := binary.Read(file, binary.LittleEndian, &bf.k); err != nil {
		return err
	}
	if err := binary.Read(file, binary.LittleEndian, &bf.m); err != nil {
		return err
	}

	bf.bitset = make([]bool, bf.m)

	buffer := make([]byte, 4096) // buffer za citanje bajtova
	bitCount := 0
	for {
		n, err := file.Read(buffer)
		if err != nil && err.Error() != "EOF" {
			return err
		}

		for byteIndex := 0; byteIndex < n; byteIndex++ {
			for bitIndex := 0; bitIndex < 8; bitIndex++ {
				if bitCount >= int(bf.m) { // prekini ako je bitset popunjen
					return nil
				}
				// 1 << bitIndex radi pomjeranje ulijevo za bitIndex
				// operator & je logicno AND kada se uradi izmedju trenutnog byte sa kojm radimo i maskom
				// odredjujemo da li je na tom mjestu bila jedinica ili nula
				// ako dobijemo rezultat razlicit od nula znaci da je tu bila jedinica, a poredjenje ce vratiti true znaci bice upisano 1
				// u suprotnom, ako je nakon AND operacije sve nule, znaci da je i na tom mjesu bila nula, poredjene != 0 vraca false i upisuje se nula
				bf.bitset[bitCount] = (buffer[byteIndex] & (1 << bitIndex)) != 0
				bitCount++
			}
		}

		if err == io.EOF {
			break
		}
	}

	for {
		seed := make([]byte, 4)
		if _, err := file.Read(seed); err != nil {
			return err // ako se desi greska, takodje i EOF predstavlja gresku, prekida se ova beskonacna petlja
		}
		bf.hashFuncs = append(bf.hashFuncs, HashWithSeed{Seed: seed})
	}

	return nil
}

/*func main() {
	bf := createBloomFilter(1000, 0.01)
	bf2 := bf
	bf.addElement("jabuka")
	bf.addElement("banana")
	bf.addElement("kruska")

	fmt.Println(bf.check("jabuka"))
	fmt.Println(bf.check("salata"))

	bf.serialize("bloomSerialize.bin")

	bf2.deserialize("bloomSerialize.bin")

	fmt.Println(bf2.check("jabuka"))
	fmt.Println(bf2.check("salata"))

	bf.delete()
	if bf.bitset == nil && bf.hashFuncs == nil && bf.k == 0 && bf.m == 0 {
		fmt.Println("Fajl je obrisan")
	}
}*/
