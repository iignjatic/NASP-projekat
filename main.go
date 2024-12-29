package main

import (
	"fmt"
	"nasp/simhash"
)

func main() {
	filePath1 := "tekst1.txt"
	filePath2 := "tekst2.txt"

	text1, err := simhash.ReadFile(filePath1)
	if err != nil {
		fmt.Printf("Error reading file %s: %v\n", filePath1, err)
		return
	}

	text2, err := simhash.ReadFile(filePath2)
	if err != nil {
		fmt.Printf("Error reading file %s: %v\n", filePath2, err)
		return
	}

	hash := simhash.NewSimHash(8)

	words1 := simhash.SplitAndClean(text1)
	weights1 := simhash.CountWordOccurences(words1)
	table1 := hash.MakeWeightsVector(weights1)
	fingerprint1 := hash.GenerateFingerprint(table1)

	words2 := simhash.SplitAndClean(text2)
	weights2 := simhash.CountWordOccurences(words2)
	table2 := hash.MakeWeightsVector(weights2)
	fingerprint2 := hash.GenerateFingerprint(table2)

	hemingDistance := simhash.GetHammingsDistance(fingerprint1, fingerprint2)

	fmt.Printf("Result for file %s: [%v]\n", filePath1, fingerprint1)
	fmt.Printf("Result for file2 %s: [%v]\n", filePath2, fingerprint2)
	fmt.Printf("Hemming Distance: [%v]", hemingDistance)
}
