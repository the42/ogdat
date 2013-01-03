package ogdatv21

import (
	"encoding/csv"
	"fmt"
	"github.com/the42/ogdat"
	"io"
	"log"
	"os"
	"reflect"
	"regexp"
	"unicode/utf8"
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

var regexphtmlcodecheck = regexp.MustCompile(`<\w+.*('|"|)>`)
var regexphtmlescape = regexp.MustCompile(`&\w{1,10};|&#\d{1,6};`)
var regexpurlencode = regexp.MustCompile(`%[0-9a-fA-F][0-9a-fA-F]`)
var regexpposixescape = regexp.MustCompile(`\\n|\\b|\\v|\\t`)

// return values are:
// status: <> 0 indicates sthg. was wrong, // 1 = Info, 2 = Warning, 3 = Error
// position > -1: position of offending input in string, only set if status <> 0
// message: clear text of reason why the input string failes to be a correct OGD string
func CheckOGDTextStringForSaneCharacters(str string) (status, position int, message string) {
	if !utf8.ValidString(str) {
		return 3, -1, "Zeichenfolge ist nicht als UTF8 kodiert"
	}
	if idx := regexphtmlcodecheck.FindIndex([]byte(str)); idx != nil {
		return 2, idx[0], fmt.Sprintf("Mögliche HTML-Sequenz: '%s'", str[idx[0]:min(20, idx[1]-idx[0])])
	}
	if idx := regexphtmlescape.FindIndex([]byte(str)); idx != nil {
		return 2, idx[0], fmt.Sprintf("Mögliche HTML-Escapes: '%s'", str[idx[0]:min(15, idx[1]-idx[0])])
	}
	if idx := regexpurlencode.FindIndex([]byte(str)); idx != nil {
		return 2, idx[0], fmt.Sprintf("Mögliche Url-Escapes: '%s'", str[idx[0]:min(8, idx[1]-idx[0])])
	}
	if idx := regexpposixescape.FindIndex([]byte(str)); idx != nil {
		return 2, idx[0], fmt.Sprintf("Mögliche Posix-Escapes: '%s'", str[idx[0]:min(5, idx[1]-idx[0])])
	}
	return
}

func (md *MetaData) Check() (message []ogdat.CheckMessage, err error) {
	const pflichtfeldfehlt = "Pflichtfeld nicht gesetzt"

	ogdset := ogdat.GetOGDSetForVersion(Version)
	if ogdset == nil {
		return nil, fmt.Errorf("Beschreibung für OGD Version %s ist nicht vorhanden, check kann nicht durchgeführt werden", Version)
	}

nextbeschreibung:
	for _, elm := range ogdset.Beschreibung {
		if elm.IsRequired() && elm.Anzahl != "N" { // check required fields for their existence. However, if the cardinality is defined
			// as 'N', it may be ok that the field is not present, in which case we check explicitely and issue a warning
			ielements := reflect.TypeOf(md).Elem().NumField()
			for i := 0; i < ielements; i++ {
				f := reflect.TypeOf(md).Elem().Field(i)
				if ogdat.GetIDFromMetaDataStructField(f) == elm.ID && ogdat.MetaDataStructFieldIsNil(f) {
					message = append(message, ogdat.CheckMessage{Type: 3, OGDID: ogdat.GetIDFromMetaDataStructField(f), Text: pflichtfeldfehlt})
					break nextbeschreibung // required field is not present - nothing more to check
				}
			}
		}

		switch elm.OGD_Kurzname {
		case "metadata_identifier":
			if md.Extras.Metadata_Identifier.UUID == nil {
				message = append(message, ogdat.CheckMessage{
					Type:  3,
					OGDID: elm.ID,
					Text:  fmt.Sprintf("Feldwert vom Typ UUID erwartet, Wert ist aber keine UUID: '%s'", md.Extras.Metadata_Identifier.Raw)})
			}
		case "metadata_modified":
			if md.Extras.Metadata_Modified.Format != CustomTimeSpecifier2 {
				message = append(message, ogdat.CheckMessage{
					Type:  3,
					OGDID: elm.ID,
					Text:  fmt.Sprintf("Feldwert vom Typ ÖNORM ISO 8601 YYYY-MM-DD erwartet, Wert entspricht aber nicht diesem Typ: '%s'", md.Extras.Metadata_Modified.Raw)})
			}
		case "title":
			if err, idx, msg := CheckOGDTextStringForSaneCharacters(*md.Title); err > 0 {
				message = append(message, ogdat.CheckMessage{
					Type:  err,
					OGDID: elm.ID,
					Text:  fmt.Sprintf("Zeichenfolge enthält potentiell ungeeignete Zeichen ab Position %d: %s", idx, msg)})
			}
		case "description":
			if err, idx, msg := CheckOGDTextStringForSaneCharacters(*md.Description); err > 0 {
				message = append(message, ogdat.CheckMessage{
					Type:  err,
					OGDID: elm.ID,
					Text:  fmt.Sprintf("Zeichenfolge enthält potentiell ungeeignete Zeichen ab Position %d: %s", idx, msg)})
			}
		case "categorization":
			if cat := md.Extras.Categorization; cat == nil {
				message = append(message, ogdat.CheckMessage{
					Type:  2,
					OGDID: elm.ID,
					Text:  "Kategorisierung darf zwar mit Karidnalität 'N' auftreten, jedoch sollte zumindest eine Zuordnung getroffen werden"})

			} else {
				for _, element := range cat {
					if element.NumID == -1 {
						message = append(message, ogdat.CheckMessage{
							Type:  3,
							OGDID: elm.ID,
							Text:  fmt.Sprintf("Die Kategorie '%s' ist keine normierte OGD-Kategorie", element.ID)})
					}
				}
			}
		}
	}
	return
}

func init() {
	isolangfilemap = loadisolanguagefile(iso639file)
}
