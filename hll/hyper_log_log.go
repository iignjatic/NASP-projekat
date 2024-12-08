package main

//POKUSATI DODATNO POVECATI PRECIZNOST
import (
	"fmt"
	"hash/fnv"
	"math"
	"math/bits"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

func firstKbits(value, k uint64) uint64 { //izdvaja prvih k bitova iz binarne reprezentacije vrijednosti value,
	//tako što pomjera broj za 64 - k mesta udesno, čime "odsijeca" ostatak bitova.
	return value >> (64 - k)
}

func trailingZeroBits(value uint64) int { //vraca broj nula na kraju binarne reprezentacije vrijednosti value
	return bits.TrailingZeros64(value)

}

type HLL struct {
	m   uint64  //velicina seta
	p   uint8   //koliko vodecih bitova se koristi za registar
	reg []uint8 //niz registara, svaki cuva najveci broj uzastopnih nula na kraju hash vrijednosti za razlicite elemente
}

func (hll *HLL) Estimate() float64 { //procjena broja jedinstvenih elemenata
	sum := 0.0
	for _, val := range hll.reg {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m)) //normalizacioni faktor  koji zavisi od m,utvrdjene vrijednosti
	// koje minimizuju greske algoritma
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum //procjena kardinalnosti, koristi harmonijsku sredinu
	emptyRegs := hll.emptyCount()                             //prazni registri su znak male kardinalnosti, skup elemenata je mali jer
	//hash funkcija nije uspjela da popuni sve registre
	if estimation <= 2.5*float64(hll.m) { //ako je procjena manja od 2.5 × m, koristi se Harmonička sredina.
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { //Ako je procjena vrlo velika, koristi se korekcija zasnovana
		//na broju zauzetih registara.
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HLL) emptyCount() int { //broji prazne registre
	sum := 0
	for _, val := range hll.reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

// func hashFunc(value interface{}) uint64 { //tip interface{} znaci da funkcija moze primiti bilo koji tip kao parametar
//
//		hasher := fnv.New64()
//		fmt.Fprintf(hasher, "%v", value)
//		return hasher.Sum64()
//	}
func hashFunc(value interface{}) uint64 {
	hasher := fnv.New64a()
	fmt.Fprintf(hasher, "%v", value) // Ovo može biti nesigurno kod određenih tipova podataka
	return hasher.Sum64()
}

func (hll *HLL) AddElement(value interface{}) {
	hash_value := hashFunc(value)
	//index := firstKbits(hash_value, uint64(hll.p)) //u koji registar smjestam procjenjenu vrijednost
	index := firstKbits(hash_value, uint64(hll.p))

	zeroBits := trailingZeroBits(hash_value) //broj uzastopnih nula
	if zeroBits > int(hll.reg[index]) {
		hll.reg[index] = uint8(zeroBits)
	}

}

func main() {
	p := uint8(6)
	m := math.Pow(2.0, float64(p))
	hll := &HLL{
		m:   uint64(m),
		p:   p,
		reg: make([]uint8, int(m)),
	}
	for i := 1; i < 1000000; i++ { //za 1 000 000 razlicitih elemenata, pri p = 6, daje 573963.11 procjenu
		hll.AddElement(i)
	}
	// hll.AddElement(5)
	// hll.AddElement(8)
	// hll.AddElement(12)
	// hll.AddElement(5)
	// hll.AddElement(5)
	// hll.AddElement(5)
	// hll.AddElement(12)
	// hll.AddElement(20)
	// hll.AddElement(80)
	// hll.AddElement(40)
	// hll.AddElement(36)
	// hll.AddElement(550)
	// hll.AddElement(85828)
	// hll.AddElement(477)
	// hll.AddElement(7710)

	fmt.Printf("Procjena broja jedinstvenih elemenata: %.2f\n", hll.Estimate())
}
