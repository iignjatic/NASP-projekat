package main

import "fmt"

func main() {
	/* FORMIRANJE STRUKTURA */

	/*
		- memtable
		- wal
		- cache
		- sstable

	*/

	var input uint32
	var key string
	var value []byte

	for {
		fmt.Println(" * KEY - VALUE ENGINE * ")
		fmt.Println("Izaberite opciju: ")
		fmt.Println("1. GET [ key ] ")
		fmt.Println("2. PUT [ key, value] ")
		fmt.Println("3. DELETE [ key ]")

		fmt.Scan(&input)

		if input == 1 {
			//GET operacija

			fmt.Scan(&key)

			/* if SEARCHMEMTABLE != nil
				continue

			else if SEARCHCACHE != nil
				continue

			else if SEARCHSSTABLE != nil
				found key

			else
				not found


			*/
		} else if input == 2 {
			//PUT OPERACIJA

			fmt.Scan(&key, &value)

			/*
				writeToWAL

				writeToMEM

				if mem is full
					writeSSTable

			*/

		} else if input == 3 {

			//DELETE OPERACIJA
			fmt.Scan(&key)

			/*
				updateWAL

				updateMEM

			*/

		}
	}

}
