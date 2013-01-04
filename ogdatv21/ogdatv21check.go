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

type CheckError struct {
	Status, Position int
	message          string
}

func (ce *CheckError) Error() string {
	return ce.message
}

var regexphtmlcodecheck = regexp.MustCompile(`<\w+.*('|"|)>`)
var regexphtmlescape = regexp.MustCompile(`&\w{1,10};|&#\d{1,6};`)
var regexpurlencode = regexp.MustCompile(`%[0-9a-fA-F][0-9a-fA-F]`)
var regexpposixescape = regexp.MustCompile(`\\n|\\b|\\v|\\t`)

// return values are:
// ok = false indicates sthg. was wrong in which case error will not be nil
//
// error: if is of type CheckError:
// Status: 1 = Info, 2 = Warning, 3 = Error
// Position: beginning position of offending input
// message: An error message describing the problem
func CheckOGDTextStringForSaneCharacters(str string) (ok bool, _ error) {
	if !utf8.ValidString(str) {
		return false, &CheckError{3, -1, "Zeichenfolge ist nicht durchgängig gültig als UTF8 kodiert"}
	}
	if idx := regexphtmlcodecheck.FindIndex([]byte(str)); idx != nil {
		return false, &CheckError{2, idx[0], fmt.Sprintf("Mögliche HTML-Sequenz: '%s'", str[idx[0]:min(20, idx[1]-idx[0])])}
	}
	if idx := regexphtmlescape.FindIndex([]byte(str)); idx != nil {
		return false, &CheckError{2, idx[0], fmt.Sprintf("Mögliche HTML-Escapes: '%s'", str[idx[0]:min(15, idx[1]-idx[0])])}
	}
	if idx := regexpurlencode.FindIndex([]byte(str)); idx != nil {
		return false, &CheckError{2, idx[0], fmt.Sprintf("Mögliche Url-Escapes: '%s'", str[idx[0]:min(8, idx[1]-idx[0])])}
	}
	if idx := regexpposixescape.FindIndex([]byte(str)); idx != nil {
		return false, &CheckError{2, idx[0], fmt.Sprintf("Mögliche Posix-Escapes: '%s'", str[idx[0]:min(5, idx[1]-idx[0])])}
	}
	return true, nil
}

var regexpbboxWKT = regexp.MustCompile(`^POLYGON\s{0,1}\({1,2}\s{0,2}[-+]?[0-9]*\.?[0-9]+\s{1,2}[-+]?[0-9]*\.?[0-9]+,\s{0,2}[-+]?[0-9]*\.?[0-9]+\s{1,2}[-+]?[0-9]*\.?[0-9]+\s{0,2}\){1,2}$`)

func CheckOGDBBox(str string) (bool, error) {
	if !utf8.ValidString(str) {
		return false, &CheckError{3, -1, "Zeichenfolge ist nicht durchgängig gültig als UTF8 kodiert"}
	}
	if idx := regexpbboxWKT.FindIndex([]byte(str)); idx == nil {
		return false, &CheckError{3, -1, "Keine gültige WKT-Angabe einer BoundingBox"}
	}
	return true, nil
}

func (md *MetaData) Check() (message []ogdat.CheckMessage, err error) {
	const pflichtfeldfehlt = "Pflichtfeld nicht gesetzt"

	ogdset := ogdat.GetOGDSetForVersion(Version)
	if ogdset == nil {
		return nil, fmt.Errorf("Beschreibung für OGD Version %s ist nicht vorhanden, check kann nicht durchgeführt werden", Version)
	}

nextbeschreibung:
	for _, elm := range ogdset.Beschreibung {

		// check required fields for their presence. However, if the
		// cardinality on a required fiels is defined as 'N', it may be ok
		// that the field is not present, in which case we check
		// later explicitely and issue a warning
		if elm.IsRequired() && elm.Anzahl != "N" {
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
					Text:  fmt.Sprintf("Feldwert vom Typ ÖNORM ISO 8601 'YYYY-MM-DD' erwartet, Wert entspricht aber nicht diesem Typ: '%s'", md.Extras.Metadata_Modified.Raw)})
			}
		case "title":
			if ok, err := CheckOGDTextStringForSaneCharacters(*md.Title); !ok {
				if cerr, ok := err.(*CheckError); ok {
					message = append(message, ogdat.CheckMessage{
						Type:  cerr.Status,
						OGDID: elm.ID,
						Text:  fmt.Sprintf("Zeichenfolge enthält potentiell ungeeignete Zeichen ab Position %d: '%s'", cerr.Position, cerr)})
				}
			}
		case "description":
			if ok, err := CheckOGDTextStringForSaneCharacters(*md.Description); !ok {
				if cerr, ok := err.(*CheckError); ok {
					message = append(message, ogdat.CheckMessage{
						Type:  cerr.Status,
						OGDID: elm.ID,
						Text:  fmt.Sprintf("Zeichenfolge enthält potentiell ungeeignete Zeichen ab Position %d: '%s'", cerr.Position, cerr)})
				}
			}
		case "categorization":
			if cat := md.Extras.Categorization; cat == nil {
				message = append(message, ogdat.CheckMessage{
					Type:  2,
					OGDID: elm.ID,
					Text:  "Die Kategorisierung darf zwar mit Kardinalität 'N' optional auftreten, jedoch sollte zumindest eine Zuordnung getroffen werden"})

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
		case "keywords":
			if keywords := md.Schlagworte; keywords == nil {
				message = append(message, ogdat.CheckMessage{
					Type:  2,
					OGDID: elm.ID,
					Text:  "Schlagworte dürfen zwar mit Kardinalität 'N' optional auftreten, die Angabe von Schlagworten wäre aber wünschenswert"})

			}
		case "maintainer":
			if ok, err := CheckOGDTextStringForSaneCharacters(*md.Maintainer); !ok {
				if cerr, ok := err.(*CheckError); ok {
					message = append(message, ogdat.CheckMessage{
						Type:  cerr.Status,
						OGDID: elm.ID,
						Text:  fmt.Sprintf("Zeichenfolge enthält potentiell ungeeignete Zeichen ab Position %d: '%s'", cerr.Position, cerr)})
				}
			}
		case "license":
			if ok, err := CheckOGDTextStringForSaneCharacters(*md.License); !ok {
				if cerr, ok := err.(*CheckError); ok {
					message = append(message, ogdat.CheckMessage{
						Type:  cerr.Status,
						OGDID: elm.ID,
						Text:  fmt.Sprintf("Zeichenfolge enthält potentiell ungeeignete Zeichen ab Position %d: '%s'", cerr.Position, cerr)})
				}
			}
		case "begin_datetime":
			if md.Extras.Begin_DateTime.Format != CustomTimeSpecifier1 {
				message = append(message, ogdat.CheckMessage{
					Type:  3,
					OGDID: elm.ID,
					Text:  fmt.Sprintf("Feldwert vom Typ ÖNORM ISO 8601 TM_Primitive 'YYYY-MM-DDThh:mm:ss' erwartet, Wert entspricht aber nicht diesem Typ: '%s'", md.Extras.Begin_DateTime.Raw)})
			}
			// ###################### OPTIONALE FELDER ######################
		case "schema_name":
			if schemaname := md.Extras.Schema_Name; schemaname != nil {
				if ok, err := CheckOGDTextStringForSaneCharacters(*schemaname); !ok {
					if cerr, ok := err.(*CheckError); ok {
						message = append(message, ogdat.CheckMessage{
							Type:  cerr.Status,
							OGDID: elm.ID,
							Text:  fmt.Sprintf("Zeichenfolge enthält potentiell ungeeignete Zeichen ab Position %d: '%s'", cerr.Position, cerr)})
					}
					const ogdschemaspec = "OGD Austria Metadata 2.0"
					if *schemaname != ogdschemaspec {
						message = append(message, ogdat.CheckMessage{
							Type:  1,
							OGDID: elm.ID,
							Text:  fmt.Sprintf("Schemabezeichnung als '%s' erwartet, der Wert ist aber '%s'", ogdschemaspec, *schemaname)})
					}
				}
			}
		case "schema_language":
			if lang := md.Extras.Schema_Language; lang != nil {
				const ogdschemalanguage = "ger"
				if *lang != ogdschemalanguage {
					message = append(message, ogdat.CheckMessage{
						Type:  3,
						OGDID: elm.ID,
						Text:  fmt.Sprintf("Schemasprache als '%s' erwartet, der Wert ist aber '%s'", ogdschemalanguage, *lang)})
				}
			}
		case "schema_characterset":
			if charset := md.Extras.Schema_Characterset; charset != nil {
				const ogdschemacharacterset = "utf8"
				if *charset != ogdschemacharacterset {
					message = append(message, ogdat.CheckMessage{
						Type:  3,
						OGDID: elm.ID,
						Text:  fmt.Sprintf("Characterset des Schemas als '%s' erwartet, der Wert ist aber '%s'", ogdschemacharacterset, *charset)})
				}
			}
		case "metadata_linkage":
			for _, element := range md.Extras.Metadata_Linkage {
				if element.URL == nil {
					message = append(message, ogdat.CheckMessage{
						Type:  3,
						OGDID: elm.ID,
						Text:  fmt.Sprintf("Gültigen Verweis (Link) erwartet, der Wert '%s' stellt keinen gültigen Link dar", element.Raw)})
				}
			}
		case "attribute_description":
			if desc := md.Extras.Attribute_Description; desc != nil {
				const ogddesclen = 20
				if i := len(*desc); i < ogddesclen {
					message = append(message, ogdat.CheckMessage{
						Type:  2,
						OGDID: elm.ID,
						Text:  fmt.Sprintf("Beschreibung enthält weniger als %d Zeichen", i)})

				}
				if ok, err := CheckOGDTextStringForSaneCharacters(*desc); !ok {
					if cerr, ok := err.(*CheckError); ok {
						message = append(message, ogdat.CheckMessage{
							Type:  cerr.Status,
							OGDID: elm.ID,
							Text:  fmt.Sprintf("Zeichenfolge enthält potentiell ungeeignete Zeichen ab Position %d: '%s'", cerr.Position, cerr)})
					}
				}
			}
		case "maintainer_link":
			if link := md.Extras.Maintainer_Link; link != nil {
				if link.URL == nil {
					message = append(message, ogdat.CheckMessage{
						Type:  3,
						OGDID: elm.ID,
						Text:  fmt.Sprintf("Gültigen Verweis (Link) erwartet, der Wert '%s' stellt keinen gültigen Link dar", link.Raw)})
				}
			}
		case "publisher":
			if publisher := md.Extras.Publisher; publisher != nil {
				if ok, err := CheckOGDTextStringForSaneCharacters(*publisher); !ok {
					if cerr, ok := err.(*CheckError); ok {
						message = append(message, ogdat.CheckMessage{
							Type:  cerr.Status,
							OGDID: elm.ID,
							Text:  fmt.Sprintf("Zeichenfolge enthält potentiell ungeeignete Zeichen ab Position %d: '%s'", cerr.Position, cerr)})
					}
				}
			}
		case "geographic_toponym":
			if toponym := md.Extras.Geographich_Toponym; toponym != nil {
				if ok, err := CheckOGDTextStringForSaneCharacters(*toponym); !ok {
					if cerr, ok := err.(*CheckError); ok {
						message = append(message, ogdat.CheckMessage{
							Type:  cerr.Status,
							OGDID: elm.ID,
							Text:  fmt.Sprintf("Zeichenfolge enthält potentiell ungeeignete Zeichen ab Position %d: '%s'", cerr.Position, cerr)})
					}
				}
			}
		case "geographic_bbox":
			if bbox := md.Extras.Geographic_BBox; bbox != nil {
				if ok, err := CheckOGDBBox(*bbox); !ok {
					message = append(message, ogdat.CheckMessage{
						Type:  3,
						OGDID: elm.ID,
						Text:  fmt.Sprintf("Zeichenfolge enthält keinen gültigen WKT für die örtliche Begrenzung (Boundingbox): '%s'", err)})
				}
			}
		case "end_datetime":
			if endtime := md.Extras.End_DateTime; endtime != nil {
				if endtime.Format != CustomTimeSpecifier1 {
					message = append(message, ogdat.CheckMessage{
						Type:  3,
						OGDID: elm.ID,
						Text:  fmt.Sprintf("Feldwert vom Typ ÖNORM ISO 8601 TM_Primitive 'YYYY-MM-DDThh:mm:ss' erwartet, Wert entspricht aber nicht diesem Typ: '%s'", endtime.Raw)})
				}
			}
		case "update_frequency":
			if frequency := md.Extras.Update_Frequency; frequency != nil {

			}
		}
	}
	return

	/*
		 		case "resource_url":
			  if keywords := md.Resources.Url; keywords == nil {
			  message = append(message, ogdat.CheckMessage{
						Type:  2,
						OGDID: elm.ID,
						Text:  "Schlagworte dürfen zwar mit Karidnalität 'N' optional auftreten, die Angabe von Schlagworten wäre aber wünschenswert"})
	*/
}

func init() {
	isolangfilemap = loadisolanguagefile(iso639file)
}
