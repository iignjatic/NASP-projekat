package main

import (
	"container/list"
	"fmt"
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

type cacheElement struct {
	key   string
	value []byte
}

// funkcija makeLRUCache pravi novi LruCache sa datim kapacitetom

func makeLRUCache(capacity int) *LruCache {
	return &LruCache{
		capacity:  capacity,
		cacheMap:  make(map[string]*list.Element),
		cacheList: list.New(),
	}
}

// funkcija get vraca vrijednost elementa kesa sa datim kljucem
// vraca niz bajtova a prima kljuc, ako element ne postoji pod datim kljucem vraca nil

func (lru *LruCache) get(key string) []byte {
	if element, exists := lru.cacheMap[key]; exists {
		lru.cacheList.MoveToFront(element)
		return element.Value.(*cacheElement).value
	}
	return nil
}

// funkcija put dodaje novi element u kes, prima kljuc i vrijednost elementa
// ako element vec postoji pod datim kljucem, mijenja vrijednost elementa
// ako je kes pun, uklanja posljednji element

func (lru *LruCache) put(key string, value []byte) {
	if element, exists := lru.cacheMap[key]; exists {
		element.Value.(*cacheElement).value = value
		lru.cacheList.MoveToFront(element)
		return
	}

	newElement := &cacheElement{key: key, value: value}
	element := lru.cacheList.PushFront(newElement)
	lru.cacheMap[key] = element

	if lru.cacheList.Len() > lru.capacity {
		lastElement := lru.cacheList.Back()
		if lastElement != nil {
			delete(lru.cacheMap, lastElement.Value.(*cacheElement).key)
			lru.cacheList.Remove(lastElement)
		}
	}
}

// testiram lru cache

func main() {
	lru := makeLRUCache(2)
	lru.put("a", []byte("vrijednost_a"))
	lru.put("b", []byte("vrijednost_b"))
	fmt.Println(string(lru.get("a")))
	lru.put("c", []byte("vrijednost_c"))
	fmt.Println(string(lru.get("b")))
}
