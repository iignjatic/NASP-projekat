package lruCache // ovo sam promjenio iz main u lruCache

import (
	"container/list"
)

// struktura LruCache sadrzi kapacitet lru kesa,
// mapu koja sadrzi kljuceve i pokazivace na elemente liste,
// i listu koja sadrzi elemente kesa

type LruCache struct {
	capacity  int
	cacheMap  map[string]*list.Element
	cacheList *list.List
}

// struktura cacheElement sadrzi kljuc i vrijednost elementa kesa

type CacheElement struct {
	key   string
	value []byte
}

// funkcija makeLRUCache pravi novi LruCache sa datim kapacitetom

func MakeLRUCache(capacity int) *LruCache {
	return &LruCache{
		capacity:  capacity,
		cacheMap:  make(map[string]*list.Element),
		cacheList: list.New(),
	}
}

// funkcija get vraca vrijednost elementa kesa sa datim kljucem
// vraca niz bajtova a prima kljuc, ako element ne postoji pod datim kljucem vraca nil

func (lru *LruCache) Get(key string) []byte {
	if element, exists := lru.cacheMap[key]; exists {
		lru.cacheList.MoveToFront(element)
		return element.Value.(*CacheElement).value
	}
	return nil
}

// funkcija put dodaje novi element u kes, prima kljuc i vrijednost elementa
// ako element vec postoji pod datim kljucem, mijenja vrijednost elementa
// ako je kes pun, uklanja posljednji element

func (lru *LruCache) Put(key string, value []byte) {
	if element, exists := lru.cacheMap[key]; exists {
		element.Value.(*CacheElement).value = value
		lru.cacheList.MoveToFront(element)
		return
	}
	element := lru.cacheList.PushFront(&CacheElement{key: key, value: value})
	lru.cacheMap[key] = element

	if lru.cacheList.Len() > lru.capacity {
		if lastElement := lru.cacheList.Back(); lastElement != nil {
			if lastCacheElement, ok := lastElement.Value.(*CacheElement); ok {
				delete(lru.cacheMap, lastCacheElement.key)
				lru.cacheList.Remove(lastElement)
			}
		}
	}

}

// funkcija koja uklanja element iz lru kesa, po zadatam kljucu
func (lru *LruCache) Delete(key string) {
	if element, exists := lru.cacheMap[key]; exists {
		lru.cacheList.Remove(element)
		delete(lru.cacheMap, key)
	}
}

// testiram lru cache

// func main() {
// 	lru := makeLRUCache(2)
// 	lru.Put("a", []byte("vrijednost_a"))
// 	lru.Put("b", []byte("vrijednost_b"))
//  lru.Delete("a")
// 	fmt.Println(string(lru.get("a")))
// 	lru.Put("c", []byte("vrijednost_c"))
// 	fmt.Println(string(lru.get("b")))

// }
