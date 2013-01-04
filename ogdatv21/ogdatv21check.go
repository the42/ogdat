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

func (md *MetaData) Check(checklinks bool) (message []ogdat.CheckMessage, err error) {
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
			if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*md.Title); !ok {
				if cerr, ok := err.(*ogdat.CheckError); ok {
					message = append(message, ogdat.CheckMessage{
						Type:  cerr.Status,
						OGDID: elm.ID,
						Text:  fmt.Sprintf("Zeichenfolge enthält potentiell ungeeignete Zeichen ab Position %d: '%s'", cerr.Position, cerr)})
				}
			}
		case "description":
			if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*md.Description); !ok {
				if cerr, ok := err.(*ogdat.CheckError); ok {
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
			if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*md.Maintainer); !ok {
				if cerr, ok := err.(*ogdat.CheckError); ok {
					message = append(message, ogdat.CheckMessage{
						Type:  cerr.Status,
						OGDID: elm.ID,
						Text:  fmt.Sprintf("Zeichenfolge enthält potentiell ungeeignete Zeichen ab Position %d: '%s'", cerr.Position, cerr)})
				}
			}
		case "license":
			if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*md.License); !ok {
				if cerr, ok := err.(*ogdat.CheckError); ok {
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
				if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*schemaname); !ok {
					if cerr, ok := err.(*ogdat.CheckError); ok {
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
				if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*desc); !ok {
					if cerr, ok := err.(*ogdat.CheckError); ok {
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
				if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*publisher); !ok {
					if cerr, ok := err.(*ogdat.CheckError); ok {
						message = append(message, ogdat.CheckMessage{
							Type:  cerr.Status,
							OGDID: elm.ID,
							Text:  fmt.Sprintf("Zeichenfolge enthält potentiell ungeeignete Zeichen ab Position %d: '%s'", cerr.Position, cerr)})
					}
				}
			}
		case "geographic_toponym":
			if toponym := md.Extras.Geographich_Toponym; toponym != nil {
				if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*toponym); !ok {
					if cerr, ok := err.(*ogdat.CheckError); ok {
						message = append(message, ogdat.CheckMessage{
							Type:  cerr.Status,
							OGDID: elm.ID,
							Text:  fmt.Sprintf("Zeichenfolge enthält potentiell ungeeignete Zeichen ab Position %d: '%s'", cerr.Position, cerr)})
					}
				}
			}
		case "geographic_bbox":
			if bbox := md.Extras.Geographic_BBox; bbox != nil {
				if ok, err := ogdat.CheckOGDBBox(*bbox); !ok {
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
