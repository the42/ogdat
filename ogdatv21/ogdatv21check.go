package ogdatv21

import (
	"encoding/csv"
	"fmt"
	"github.com/the42/ogdat"
	"io"
	"log"
	"os"
	"reflect"
)

type ISO6392Lang struct {
	Code, Identifier string
}

const iso639file = "ISO-639-2_utf-8.txt"
const schema_langauge = "ger"
const schema_characterset = "utf8"

var isolangfilemap map[string]*ISO6392Lang

func loadisolanguagefile(filename string) (isolangfilemap map[string]*ISO6392Lang) {
	reader, err := os.Open(iso639file)

	if err == nil {
		defer reader.Close()
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
		log.Printf("Warning: Can not read ISO language records")
	}
	return
}

func (md *MetaData) Check() (message []ogdat.CheckMessage) {
	const nopflichtfeld = "Pflichtfeld nicht gesetzt"

	if md.Extras.Metadata_Identifier != nil {
		if md.Extras.Metadata_Identifier.UUID == nil {
			f, _ := reflect.TypeOf(md).Elem().FieldByName("Metadata_Identifier")
			message = append(message, ogdat.CheckMessage{Type: 3,
				OGDID: ogdat.GetIDFromMetaDataStructField(f),
				Text:  fmt.Sprintf("Feldwert vom Typ UUID erwartet, der Wert ist aber keine UUID: '%s'", md.Extras.Metadata_Identifier.Raw)})
		}
	} else {
		f, _ := reflect.TypeOf(md).Elem().FieldByName("Metadata_Identifier")
		message = append(message, ogdat.CheckMessage{Type: 3, OGDID: ogdat.GetIDFromMetaDataStructField(f), Text: nopflichtfeld})
	}
	return
}

func init() {
	isolangfilemap = loadisolanguagefile(iso639file)
}
