package SSTable

import (
	"NASP-PROJEKAT/data"
)

type Summary struct {
	First        string //prvi kljuc u indexu
	Last         string //poslednji kljuc u indexu
	SummaryTable map[string]uint32
	Sample       uint32
}

func (summary *Summary) MakeSummary(records []*data.Record, sample uint32) {
	var offset uint32 = 0
	var counter uint32 = 0
	for i := 0; i < len(records); i++ {
		indexRecordSize := records[i].KeySize + 8
		counter++
		offset += indexRecordSize
		if counter == sample {
			summary.SummaryTable[records[i].Key] = offset
			counter = 0
		}

	}
	if len(records) > 0 {
		summary.First = records[0].Key
		summary.Last = records[len(records)-1].Key
	}
	summary.Sample = sample
}
