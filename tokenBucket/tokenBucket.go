package tokenBucket

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

func Now() int64 {
	return time.Now().Unix()
}

func IsPast(stored int64) bool {
	return stored < Now()
}

// TokenBucket je struktura koja predstavlja Token Bucket algoritam

type TokenBucket struct {
	maximumTokens         int64
	currentNumberOfTokens int64
	resetInterval         int64
	lastTimeReset         int64
	mu                    sync.Mutex
}

//	kreiram novi TokenBucket sa zadatim parametrima
//
// kao argunemti se prosleđuju maksimalan broj tokena i interval resetovanja tokena
// vracam pokazivac na novi TokenBucket
func newTokenBucket(maxTokens int64, resetInteval int64) *TokenBucket {
	return &TokenBucket{
		maximumTokens:         maxTokens,
		currentNumberOfTokens: maxTokens,
		resetInterval:         resetInteval,
		lastTimeReset:         Now(),
	}
}

// resetTokens resetuje broj tokena na maksimalan broj i postavlja vrijeme posljednjeg resetovanja na trenutno vrijeme
// prima TokenBucket kao argument
func (tokenB *TokenBucket) resetTokens() {
	tokenB.currentNumberOfTokens = tokenB.maximumTokens
	tokenB.lastTimeReset = Now()
}

// getTokens uzima tokene iz TokenBucketa
// kao argumente prima TokenBucket
func (tokenB *TokenBucket) getTokens() error {
	tokenB.mu.Lock()
	defer tokenB.mu.Unlock()

	// Provjerava se da li je proslo dovoljno vremena kako bi se tokeni resetovali
	if IsPast(tokenB.lastTimeReset + tokenB.resetInterval) {
		tokenB.resetTokens()
	}

	// Ako je broj tokena 0, vraca se greska
	if tokenB.currentNumberOfTokens <= 0 {
		return errors.New("nema dovoljno tokena")
	}

	// Smajuje se broj tokena
	tokenB.currentNumberOfTokens--

	return nil
}

// SerializeState serijalizuje stanje TokenBucketa u binarni format
// kao argument prima TokenBucket
func (t *TokenBucket) SerializeState() ([]byte, error) {
	var buf bytes.Buffer

	err := binary.Write(&buf, binary.LittleEndian, t.maximumTokens)
	if err != nil {
		return nil, fmt.Errorf("greska prilikom serijalizacije maxTokens promjenljive: %v", err)
	}
	err = binary.Write(&buf, binary.LittleEndian, t.currentNumberOfTokens)
	if err != nil {
		return nil, fmt.Errorf("greska prilikom serijalizacije tokens promjenljive: %v", err)
	}
	err = binary.Write(&buf, binary.LittleEndian, t.resetInterval)
	if err != nil {
		return nil, fmt.Errorf("greska prilikom serijalizacije resetInterval promjenljive: %v", err)
	}
	err = binary.Write(&buf, binary.LittleEndian, t.lastTimeReset)
	if err != nil {
		return nil, fmt.Errorf("greska prilikom serijalizacije lastResetTime promjenljive: %v", err)
	}

	return buf.Bytes(), nil
}

// DeserializeState deserijalizuje binarni format u stanje TokenBucketa
func (t *TokenBucket) DeserializeState(data []byte) error {
	buf := bytes.NewReader(data)

	err := binary.Read(buf, binary.LittleEndian, &t.maximumTokens)
	if err != nil {
		return fmt.Errorf("greska prilikom deserijalizacije maxTokens promjenljive: %v", err)
	}
	err = binary.Read(buf, binary.LittleEndian, &t.currentNumberOfTokens)
	if err != nil {
		return fmt.Errorf("greska prilikom deserijalizacije tokens promjenljive: %v", err)
	}
	err = binary.Read(buf, binary.LittleEndian, &t.resetInterval)
	if err != nil {
		return fmt.Errorf("greska prilikom deserijalizacije resetInterval promjenljive: %v", err)
	}
	err = binary.Read(buf, binary.LittleEndian, &t.lastTimeReset)
	if err != nil {
		return fmt.Errorf("greska prilikom deserijalizacije lastResetTime pormjenljive: %v", err)
	}

	return nil
}

// SaveState cuva stanje TokenBucketa u binarni fajl
func (t *TokenBucket) SaveState(fileName string) error {
	data, err := t.SerializeState()
	if err != nil {
		return fmt.Errorf("greska prilikom serijalizacije: %v", err)
	}

	err = os.WriteFile(fileName, data, 0644)
	if err != nil {
		return fmt.Errorf("greska prilikom cuvanja u fajl: %v", err)
	}

	return nil
}

// LoadState ucitava stanje TokenBucketa iz binarnog fajla
func (t *TokenBucket) LoadState(fileName string) error {
	data, err := os.ReadFile(fileName)
	if err != nil {
		return fmt.Errorf("greska prilikom učitavanja fajla: %v", err)
	}

	err = t.DeserializeState(data)
	if err != nil {
		return fmt.Errorf("greska prilikom deserijalizacije: %v", err)
	}

	return nil
}

func main() {

	tb := newTokenBucket(10, 30)

	fmt.Println("Testiranje uzimanja tokena:")
	for i := 0; i < 12; i++ {
		err := tb.getTokens()
		if err != nil {
			fmt.Printf("Greska prilikom uzimanja tokena: %v\n", err)
		} else {
			fmt.Printf("Uspijesno uzet token! Preostali tokeni: %d\n", tb.currentNumberOfTokens)
		}
		time.Sleep(2 * time.Second)
	}

	err := tb.SaveState("token_bucket_state.bin")
	if err != nil {
		fmt.Println("Greška prilikom cuvanja stanja:", err)
	} else {
		fmt.Println("Stanje uspijesno sacuvano u fajl.")
	}

	newTB := &TokenBucket{}
	err = newTB.LoadState("token_bucket_state.bin")
	if err != nil {
		fmt.Println("Greska prilikom ucitavanja stanja:", err)
	} else {
		fmt.Println("Stanje uspijesno ucitano iz fajla.")
	}

	fmt.Println("\nTestiranje resetovanja tokena:")

	time.Sleep(35 * time.Second)

	err = newTB.getTokens()
	if err != nil {
		fmt.Printf("Greska prilikom uzimanja tokena: %v\n", err)
	} else {
		fmt.Printf("Uspijesno uzet token! Preostali tokeni: %d\n", newTB.currentNumberOfTokens)
	}

}
