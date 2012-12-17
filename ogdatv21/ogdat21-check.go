package ogdat

import (
	"encoding/csv"
	"io"
	"log"
	"os"
)

type ISO6392Lang struct {
	Code, Identifier string
}

const iso639file = "ISO-639-2_utf-8.txt"
const schema_langauge = "ger"
const schema_characterset = "utf8"

var isolangfilemap map[string]*ISO6392Lang

func init() {
	reader, err := os.Open(iso639file)
	if err == nil {
		isolangfilemap = make(map[string]*ISO6392Lang)
		csvreader := csv.NewReader(reader)
		csvreader.Comma = '|'

		for record, err := csvreader.Read(); err != io.EOF; record, err = csvreader.Read() {
			isorecord := &ISO6392Lang{Code: record[0], Identifier: record[3]}
			isolangfilemap[record[0]] = isorecord
			if len(record[1]) > 0 {
				isorecord = &ISO6392Lang{Code: record[1], Identifier: record[3]}
				isolangfilemap[record[1]] = isorecord
			}
		}
		log.Printf("Info: Read %d ISO language records", len(isolangfilemap))
	} else {
		log.Printf("Warning: Read %d ISO language records", len(isolangfilemap))
	}

}
