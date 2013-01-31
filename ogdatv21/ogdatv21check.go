package ogdatv21

import (
	// "daviddengcn/go-algs/ed" // levenshtein distance for similarity
	"fmt"
	"github.com/the42/ogdat"
	"reflect"
	"strconv"
	"strings"
)

func (md *MetaData) Check(followhttplinks bool) (message []ogdat.CheckMessage, err error) {
	const pflichtfeldfehlt = "Pflichtfeld nicht gesetzt"
	const invalidchars = "Zeichenfolge enthält potentiell ungeeignete Zeichen ab Position %d: '%s'"
	const wrongtimevalueCT1 = "Feldwert vom Typ ÖNORM ISO 8601 TM_Primitive 'YYYY-MM-DDThh:mm:ss' erwartet, Wert entspricht aber nicht diesem Typ: '%s'"
	const wrongtimevalueCT2 = "Feldwert vom Typ ÖNORM ISO 8601 'YYYY-MM-DD' erwartet, Wert entspricht aber nicht diesem Typ: '%s'"
	const expectedlink = "Gültigen Verweis (Link) erwartet, der Wert '%s' stellt keinen gültigen Link dar"

	ogdset := ogdat.GetOGDSetForVersion(Version)
	if ogdset == nil {
		return nil, fmt.Errorf("Beschreibung für OGD Version %s ist nicht vorhanden, check kann nicht durchgeführt werden", Version)
	}

	if md.Resource == nil || len(md.Resource) == 0 {
		message = append(message, ogdat.CheckMessage{Type: 4,
			Text: "Die Metadatenbeschreibung enthält keine Ressourcen"})
	}

	// (1) iterate over all resource elements
	// save to iterate here, even without range elements, bu with an else, the nesting gets unwieldly ...
	for _, element := range md.Resource {
		ielements := reflect.TypeOf(element).Elem().NumField()
		// (2) take every field in the resource element ...
		for i := 0; i < ielements; i++ {
			f := reflect.TypeOf(element).Elem().Field(i)
			// (3) ... and get the 'Beschreibung' for this field
			id := ogdat.GetIDFromMetaDataStructField(f)
			desc, _ := ogdset.GetBeschreibungForID(id)
			if desc == nil {
				return message, fmt.Errorf("Keine Beschreibung zu Feld mit ID%d", id)
			}
			// (4a) if the field is required but not present
			if desc.IsRequired() && ogdat.MetaDataStructFieldIsNil(f) {
				// report as erroneous
				message = append(message, ogdat.CheckMessage{Type: 3, OGDID: desc.ID, Text: pflichtfeldfehlt})
				continue // required field is not present - nothing more to check, continue with next field
			}
			// (4b) otherwise perform fieldwise checks within resources
			switch desc.OGD_Kurzname {
			// Pflichtfelder
			case "resource_url":
				if element.Url.URL == nil {
					message = append(message, ogdat.CheckMessage{
						Type:  3,
						OGDID: desc.ID,
						Text:  fmt.Sprintf(expectedlink, element.Url.Raw)})
					continue
				}
				if ok, err := ogdat.CheckUrlContact(element.Url.Raw, followhttplinks); !ok {
					message = append(message, ogdat.CheckMessage{
						Type:  3,
						OGDID: desc.ID,
						Text:  err.Error()})
				}
			case "resource_format":
				const checkchars = `.:/\`
				format := string(*element.Format)
				if idx := strings.IndexAny(format, checkchars); idx > -1 {
					message = append(message, ogdat.CheckMessage{
						Type:  2,
						OGDID: desc.ID,
						Text:  fmt.Sprintf("Ungültiges Zeichen '%c' (Index %d)", format[idx], idx)})
				}
				lower := strings.ToLower(format)
				if format != lower {
					message = append(message, ogdat.CheckMessage{
						Type:  2,
						OGDID: desc.ID,
						Text:  "Format darf nur in Kleinbuchstaben angegeben werden"})
				}
			// ###################### OPTIONALE FELDER ######################
			case "resource_name":
				resname := element.Name
				if resname == nil {
					continue
				}
				if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*resname); !ok {
					if cerr, ok := err.(*ogdat.CheckError); ok {
						message = append(message, ogdat.CheckMessage{
							Type:  cerr.Status,
							OGDID: desc.ID,
							Text:  fmt.Sprintf(invalidchars, cerr.Position, cerr)})
					}
				}
			case "resource_created":
				created := element.Created
				if created == nil {
					continue
				}
				if created.Format != CustomTimeSpecifier2 {
					message = append(message, ogdat.CheckMessage{
						Type:  3,
						OGDID: desc.ID,
						Text:  fmt.Sprintf(wrongtimevalueCT2, created.Raw)})

				}
			case "resource_lastmodified":
				modified := element.LastModified
				if modified == nil {
					continue
				}
				if modified.Format != CustomTimeSpecifier2 {
					message = append(message, ogdat.CheckMessage{
						Type:  3,
						OGDID: desc.ID,
						Text:  fmt.Sprintf(wrongtimevalueCT2, modified.Raw)})
				}
			case "resource_size":
				size := element.Size
				if size == nil {
					continue
				}
				if _, err := strconv.Atoi(*size); err != nil {
					message = append(message, ogdat.CheckMessage{
						Type:  3,
						OGDID: desc.ID,
						Text:  fmt.Sprintf("Nur Zahlenangaben erlaubt, Zeichenkette enthält aber nicht-Zahlenzeichen: '%s'", size)})
				}
			case "resource_language":
				lang := element.Language
				if lang == nil {
					continue
				}
				if !ogdat.CheckISOLanguage(*lang) {
					message = append(message, ogdat.CheckMessage{
						Type:  3,
						OGDID: desc.ID,
						Text:  fmt.Sprintf("'%s' ist keine gültiger dreistelliger Sprachcode nach  ISO 639-2", *lang)})
				}
			case "resource_encoding":
				resencoding := element.Encoding
				if resencoding == nil {
					continue
				}
				// the specification mentions only these encodings as valid
				var specencodings = []string{"utf-8", "utf-16", "utf-32"}
				enc := strings.ToLower(*resencoding)
				for _, val := range specencodings {
					if enc == val || strings.Replace(enc, "-", "", -1) == val {
						continue
					}
				}
				// ... but this is unfortunate, as certainly more encodings may be valid for OGD AT
				if ogdat.CheckIANAEncoding(*resencoding) {
					message = append(message, ogdat.CheckMessage{
						Type:  2,
						OGDID: desc.ID,
						Text:  fmt.Sprintf("'%s' ist kein gültiges Encoding nach Spezifiaktion, aber registiert bei IANA", *resencoding)})
					continue
				}
				// unknown encoding, report it
				message = append(message, ogdat.CheckMessage{
					Type:  3,
					OGDID: desc.ID,
					Text:  fmt.Sprintf("'%s' ist kein bekanntes Encoding für Daten", *resencoding)})
			}
		}
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
					message = append(message, ogdat.CheckMessage{Type: 3, OGDID: elm.ID, Text: pflichtfeldfehlt})
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
					Text:  fmt.Sprintf(wrongtimevalueCT2, md.Extras.Metadata_Modified.Raw)})
			}
		case "title":
			// should this tool also check for spelling mistakes?
			// Don't think so, it does only check for adherence to the specification
			if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*md.Title); !ok {
				if cerr, ok := err.(*ogdat.CheckError); ok {
					message = append(message, ogdat.CheckMessage{
						Type:  cerr.Status,
						OGDID: elm.ID,
						Text:  fmt.Sprintf(invalidchars, cerr.Position, cerr)})
				}
			}
		case "description":
			if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*md.Description); !ok {
				if cerr, ok := err.(*ogdat.CheckError); ok {
					message = append(message, ogdat.CheckMessage{
						Type:  cerr.Status,
						OGDID: elm.ID,
						Text:  fmt.Sprintf(invalidchars, cerr.Position, cerr)})
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
						Text:  fmt.Sprintf(invalidchars, cerr.Position, cerr)})
				}
			}
		case "license":
			if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*md.License); !ok {
				if cerr, ok := err.(*ogdat.CheckError); ok {
					message = append(message, ogdat.CheckMessage{
						Type:  cerr.Status,
						OGDID: elm.ID,
						Text:  fmt.Sprintf(invalidchars, cerr.Position, cerr)})
				}
			}
		case "begin_datetime":
			if md.Extras.Begin_DateTime.Format != CustomTimeSpecifier1 {
				message = append(message, ogdat.CheckMessage{
					Type:  3,
					OGDID: elm.ID,
					Text:  fmt.Sprintf(wrongtimevalueCT1, md.Extras.Begin_DateTime.Raw)})
			}
			// ###################### OPTIONALE FELDER ######################
		case "schema_name":
			schemaname := md.Extras.Schema_Name
			if schemaname == nil {
				continue
			}
			if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*schemaname); !ok {
				if cerr, ok := err.(*ogdat.CheckError); ok {
					message = append(message, ogdat.CheckMessage{
						Type:  cerr.Status,
						OGDID: elm.ID,
						Text:  fmt.Sprintf(invalidchars, cerr.Position, cerr)})
				}
				var ogdschemaspec = []string{Version, Version20, "2.0", "2.1"}
				for _, val := range ogdschemaspec {
					if strings.Contains(*schemaname, val) {
						break
					}
				}
				message = append(message, ogdat.CheckMessage{
					Type:  1,
					OGDID: elm.ID,
					Text:  fmt.Sprintf("Schemabezeichnung vorhanden, enthält keine Referenz auf Version 2.0 oder Version 2.1 '%s'", ogdschemaspec, *schemaname)})
			}
		case "schema_language":
			lang := md.Extras.Schema_Language
			if lang == nil {
				continue
			}
			const ogdschemalanguage = "ger"
			if *lang != ogdschemalanguage {
				message = append(message, ogdat.CheckMessage{
					Type:  3,
					OGDID: elm.ID,
					Text:  fmt.Sprintf("Schemasprache als '%s' erwartet, der Wert ist aber '%s'", ogdschemalanguage, *lang)})
			}
		case "schema_characterset":
			charset := md.Extras.Schema_Characterset
			if charset == nil {
				continue
			}
			const ogdschemacharacterset = "utf8"
			if *charset != ogdschemacharacterset {
				message = append(message, ogdat.CheckMessage{
					Type:  3,
					OGDID: elm.ID,
					Text:  fmt.Sprintf("Characterset des Schemas als '%s' erwartet, der Wert ist aber '%s'", ogdschemacharacterset, *charset)})
			}
		case "metadata_linkage":
			if element := md.Extras.Metadata_Linkage_single; element != nil {
				message = append(message, ogdat.CheckMessage{
					Type:  3,
					OGDID: elm.ID,
					Text:  fmt.Sprintf("JSON vom Typ 'Array of String' erwartet, es wurde jedoch ein einzelner Wert geliefert")})
			}
			for _, element := range md.Extras.Metadata_Linkage {
				if element.URL == nil {
					message = append(message, ogdat.CheckMessage{
						Type:  3,
						OGDID: elm.ID,
						Text:  fmt.Sprintf(expectedlink, element.Raw)})
				}
			}
		case "attribute_description":
			desc := md.Extras.Attribute_Description
			if desc == nil {
				continue
			}
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
						Text:  fmt.Sprintf(invalidchars, cerr.Position, cerr)})
				}
			}
		case "maintainer_link":
			link := md.Extras.Maintainer_Link
			if link == nil {
				continue
			}
			if link.URL == nil {
				message = append(message, ogdat.CheckMessage{
					Type:  3,
					OGDID: elm.ID,
					Text:  fmt.Sprintf(expectedlink, link.Raw)})
			}
		case "publisher":
			publisher := md.Extras.Publisher
			if publisher == nil {
				continue
			}
			if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*publisher); !ok {
				if cerr, ok := err.(*ogdat.CheckError); ok {
					message = append(message, ogdat.CheckMessage{
						Type:  cerr.Status,
						OGDID: elm.ID,
						Text:  fmt.Sprintf(invalidchars, cerr.Position, cerr)})
				}
			}
		case "geographic_toponym":
			toponym := md.Extras.Geographich_Toponym
			if toponym == nil {
				continue
			}
			if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*toponym); !ok {
				if cerr, ok := err.(*ogdat.CheckError); ok {
					message = append(message, ogdat.CheckMessage{
						Type:  cerr.Status,
						OGDID: elm.ID,
						Text:  fmt.Sprintf(invalidchars, cerr.Position, cerr)})
				}
			}
		case "geographic_bbox":
			bbox := md.Extras.Geographic_BBox
			if bbox == nil {
				continue
			}
			if ok, err := ogdat.CheckOGDBBox(*bbox); !ok {
				message = append(message, ogdat.CheckMessage{
					Type:  3,
					OGDID: elm.ID,
					Text:  fmt.Sprintf("Zeichenfolge enthält keinen gültigen WKT für die örtliche Begrenzung (Boundingbox): '%s'", err)})
			}
		case "end_datetime":
			endtime := md.Extras.End_DateTime
			if endtime == nil {
				continue
			}
			if endtime.Format != CustomTimeSpecifier1 {
				message = append(message, ogdat.CheckMessage{
					Type:  3,
					OGDID: elm.ID,
					Text:  fmt.Sprintf(wrongtimevalueCT1, endtime.Raw)})
			}
		case "update_frequency":
			frequency := md.Extras.Update_Frequency
			if frequency == nil {
				continue
			}
			if frequency.NumID == -1 {
				message = append(message, ogdat.CheckMessage{
					Type:  2,
					OGDID: elm.ID,
					Text:  fmt.Sprintf("Feldwert in Anlehnung an ON/EN/ISO 19115:2003 erwartet (gültige Werte sind in der OGD Spezifikation definiert), Wert entspricht aber nicht diesem Typ: '%s'", frequency.Raw)})
			}
		case "lineage_quality":
			quality := md.Extras.Lineage_Quality
			if quality == nil {
				continue
			}
			if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*quality); !ok {
				if cerr, ok := err.(*ogdat.CheckError); ok {
					message = append(message, ogdat.CheckMessage{
						Type:  cerr.Status,
						OGDID: elm.ID,
						Text:  fmt.Sprintf(invalidchars, cerr.Position, cerr)})
				}
			}
		case "en_title_and_desc":
			en_desc := md.Extras.EnTitleDesc
			if en_desc == nil {
				continue
			}
			if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*en_desc); !ok {
				if cerr, ok := err.(*ogdat.CheckError); ok {
					message = append(message, ogdat.CheckMessage{
						Type:  cerr.Status,
						OGDID: elm.ID,
						Text:  fmt.Sprintf(invalidchars, cerr.Position, cerr)})
				}
			}
		case "license_citation":
			en_desc := md.Extras.EnTitleDesc
			if en_desc == nil {
				continue
			}
			if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*en_desc); !ok {
				if cerr, ok := err.(*ogdat.CheckError); ok {
					message = append(message, ogdat.CheckMessage{
						Type:  cerr.Status,
						OGDID: elm.ID,
						Text:  fmt.Sprintf(invalidchars, cerr.Position, cerr)})
				}
			}
		}
	}
	return
}
