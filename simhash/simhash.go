package simhash

import (
	"crypto/md5"
	"fmt"
	"os"
	"strings"
)

type SimHash struct {
	numHashBits int		// Number of hash bits (e.g. 128)
}

func NewSimHash(numHashBits int) *SimHash {
	return &SimHash{
		numHashBits: numHashBits,
	}
}

// Split the text and clean stopWords
func SplitAndClean(text string) []string {
	stopWords := map[string]bool {
		"the": true, "is": true, "at": true, "of": true, "on": true, "and": true, 
    "a": true, "to": true, "in": true, "that": true, "i": true, "me": true, 
    "my": true, "myself": true, "we": true, "our": true, "ours": true, "ourselves": true, 
    "you": true, "your": true, "yours": true, "yourself": true, "yourselves": true, 
    "he": true, "him": true, "his": true, "himself": true, "she": true, "her": true, 
    "hers": true, "herself": true, "it": true, "its": true, "itself": true, "they": true, 
    "them": true, "their": true, "theirs": true, "themselves": true, "what": true, 
    "which": true, "who": true, "whom": true, "this": true, "these": true, 
    "those": true, "am": true, "are": true, "was": true, "were": true, "be": true, 
    "been": true, "being": true, "have": true, "has": true, "had": true, "having": true, 
    "do": true, "does": true, "did": true, "doing": true, "but": true, "if": true, 
    "or": true, "because": true, "as": true, "until": true, "while": true, "by": true, 
    "for": true, "with": true, "about": true, "against": true, "between": true, 
    "into": true, "through": true, "during": true, "before": true, "after": true, 
    "above": true, "below": true, "from": true, "up": true, "down": true, 
    "out": true, "off": true, "over": true, "under": true, "again": true, "further": true, 
    "then": true, "once": true, "here": true, "there": true, "when": true, "where": true, 
    "why": true, "how": true, "all": true, "any": true, "both": true, "each": true, 
    "few": true, "more": true, "most": true, "other": true, "some": true, "such": true, 
    "no": true, "nor": true, "not": true, "only": true, "own": true, "same": true, 
    "so": true, "than": true, "too": true, "very": true, "s": true, "t": true, 
    "can": true, "will": true, "just": true, "don": true, "should": true, "now": true,
    "weren't": true, "mustn't": true, "wasn't": true, "hasn't": true,
    "haven't": true, "hadn't": true, "isn't": true, "aren't": true, "doesn't": true,
	}
	// Make the array of words, all lowercase
	// Fields separate words by multiple whitespaces(newline included) into array
	allWords := strings.Fields(strings.ToLower(text))
	resultWords := []string {}
	for i:=0; i<len(allWords); i++ {
		if !stopWords[allWords[i]] {
			resultWords = append(resultWords, allWords[i])
		}
	}
	return resultWords
}

// Count weights of cleaned words
func CountWordOccurences(words []string) map[string] int {
	wordOcc := make(map[string]int)
	for i:=0; i<len(words); i++ {
		wordOcc[words[i]]++
	}
	return wordOcc
}

// Every word goes through hash function so we can get b-bit hash, here as string
func GetHashAsString(data []byte) string {
	hash := md5.Sum(data)
	res := ""
	for _, b := range hash {
		res = fmt.Sprintf("%s%b", res, b)
	}
	return res
}

// Make weights vector with the size numHashBits
func (s *SimHash) MakeWeightsVector(wordWeights map[string]int) []int {
	vector := make([]int, s.numHashBits)
	for word, weight := range wordWeights {
		// Calculate hash value for every word - hash value must have size 'numHashBits'
		hashString := GetHashAsString([]byte(word))
		// This won't affect the accuracy of SimHash, as only the number of bits used to calculate the vector and generate the fingerprint is SIGNIFICANT.
		if len(hashString) > s.numHashBits {
			hashString = hashString[:s.numHashBits]
		} else if len(hashString) < s.numHashBits {
			hashString = fmt.Sprintf("%0*s", s.numHashBits, hashString)
		}
		for i:=0; i<len(hashString); i++ {
			// When the bit is '1', the weight of that word is added at the corresponding index in the vector:
			if hashString[i] == '1' {	// When I iterate through string I get a character, not a string so I compare with '1'
				vector[i] += weight
			// When the bit is '0', the weight of that word is subtracted at the corresponding index in the vector:
			} else {
				vector[i] -= weight
			}
		}
	}
	return vector
}

// Get the b-bit fingerprint for vector
func (s *SimHash) GenerateFingerprint(vector []int) string {
	fingerprint := ""
	for i:=0; i<len(vector); i++ {
		if vector[i] > 0 {
			fingerprint += "1"
		} else {
			fingerprint += "0"
		}
	}
	return fingerprint
}

// XOR operation for comparison of two fingerprints
func GetHammingsDistance(fingerprint1, fingerprint2 string) string {
	if len(fingerprint1) != len(fingerprint2) {
		fmt.Println("Fingerprints must have the same length.")
	}
	var result string
	for i := 0; i < len(fingerprint1); i++ {
		if fingerprint1[i] == fingerprint2[i] {
			result += "0"
		} else {
			result += "1"
		}
	}
	return result
}

// Read txt file
func ReadFile(filePath string) (string, error) {
	file, err := os.ReadFile(filePath)
	if err != nil {
		return "", err
	} 
	return string(file), nil
}