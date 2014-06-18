package ogdatv22

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/the42/ogdat"
)

func strfloatequals(s1, s2 string, epsilon float64) (bool, error) {
	f1, err := strconv.ParseFloat(s1, 64)
	if err != nil {
		return false, err
	}
	f2, err := strconv.ParseFloat(s2, 64)
	if err != nil {
		return false, err
	}
	compval := f1 - f2
	if compval < 0.0 {
		compval *= -1
	}
	if compval > epsilon {
		return false, nil
	}
	return true, nil
}

// POLYGON ((-180.00 -90.00,180.00 -90.00,180.00 90.00, -180.00 90.00, -180.00 -90.00))
const wktregexppolygon = `^POLYGON\s{0,1}\(\(([-+]?\d*\.?\d+) ([-+]?\d*\.?\d+),\s{0,1}(?:[-+]?\d*\.?\d+ [-+]?\d*\.?\d+,\s{0,1}){3}([-+]?\d*\.?\d+) ([-+]?\d*\.?\d+)\s{0,1}\)\)$`

var regexpbboxWKT = regexp.MustCompile(wktregexppolygon)

func checkOGDBBox(str string) (bool, error) {
	const epsilon = 0.00000001
	if !utf8.ValidString(str) {
		return false, &ogdat.CheckInfo{ogdat.Error, -1, "Zeichenfolge ist nicht durchgängig gültig als UTF8 kodiert"}
	}
	idxnum := regexpbboxWKT.FindAllStringSubmatchIndex(str, 4)
	if idxnum == nil || len(idxnum[0]) < 10 {
		return false, &ogdat.CheckInfo{ogdat.Error, -1, fmt.Sprintf("Keine gültige POLYGON WKT Boundingbox-Angabe: %s", str)}

	}
	match1, err1 := strfloatequals(str[idxnum[0][2]:idxnum[0][3]], str[idxnum[0][6]:idxnum[0][7]], epsilon)
	match2, err2 := strfloatequals(str[idxnum[0][4]:idxnum[0][5]], str[idxnum[0][8]:idxnum[0][9]], epsilon)
	if err1 != nil || !match1 || err2 != nil || !match2 {
		return false, &ogdat.CheckInfo{ogdat.Error, -1, fmt.Sprintf("Beginn und Ende des Polygons ergeben kein geschlossenes Polygon: %s", str)}
	}

	return true, nil
}

func (md *MetaData) Check(followhttplinks bool) (message []ogdat.CheckMessage, err error) {
	const pflichtfeldfehlt = "Pflichtfeld nicht gesetzt"
	const invalidchars = "Zeichenfolge enthält potentiell ungeeignete Zeichen ab Position %d: %s"
	const wrongtimevalueCT1 = "Feldwert vom Typ ÖNORM ISO 8601 TM_Primitive 'YYYY-MM-DDThh:mm:ss' erwartet, Wert entspricht aber nicht diesem Typ: '%s'"
	const wrongtimevalueCT2 = "Feldwert vom Typ ÖNORM ISO 8601 'YYYY-MM-DD' erwartet, Wert entspricht aber nicht diesem Typ: '%s'"
	const expectedlink = "Gültigen Verweis (Link) erwartet, der Wert '%s' stellt keinen gültigen Link dar"
	const emptystring = "Zeichenkette mit Länge 0 an dieser Stelle nicht sinnvoll"

	if md == nil {
		return nil, fmt.Errorf("Verweis auf Metadaten ist nil")
	}

	ogdset := ogdat.GetOGDSetForVersion(Version)
	if ogdset == nil {
		return nil, fmt.Errorf("Beschreibung für OGD Version %s ist nicht vorhanden, check kann nicht durchgeführt werden", Version)
	}

	if md.Resource == nil || len(md.Resource) == 0 {
		message = append(message, ogdat.CheckMessage{Type: ogdat.Error, OGDID: -1,
			Text: "Die Metadatenbeschreibung enthält keine Ressourcen"})
	}

	// (1) iterate over all resource elements
	for iresource, element := range md.Resource {
		resourceno := fmt.Sprintf("R%4d: ", iresource)
		ielements := reflect.TypeOf(element).NumField()
		// (2) take every field in the resource element ...
		for i := 0; i < ielements; i++ {
			f := reflect.TypeOf(element).Field(i)
			// (3) ... and get the 'Beschreibung' for this field
			id := ogdat.GetIDFromMetaDataStructField(f)
			desc, _ := ogdset.GetBeschreibungForID(id)
			if desc == nil {
				return message, fmt.Errorf("Keine Beschreibung zu Feld mit ID%d", id)
			}
			fval := reflect.ValueOf(element).Field(i)
			// (4a) if the field is required but not present
			if desc.IsRequired() && fval.Kind() == reflect.Ptr && fval.IsNil() {
				// report as erroneous
				message = append(message, ogdat.CheckMessage{Type: ogdat.Error,
					OGDID: desc.ID,
					Text:  resourceno + pflichtfeldfehlt})
				continue // required field is not present - nothing more to check, continue with next field
			}
			// (4b) otherwise perform fieldwise checks within resources
			switch desc.OGD_Kurzname {
			// Pflichtfelder
			case "resource_url":
				if element.Url.URL == nil {
					message = append(message, ogdat.CheckMessage{
						Type:  ogdat.Error,
						OGDID: desc.ID,
						Text:  resourceno + fmt.Sprintf(expectedlink, element.Url.Raw)})
					continue
				}
				_, checkresult := ogdat.CheckUrl(element.Url.Raw, followhttplinks)
				message = ogdat.AppendcheckerrorTocheckmessage(message, checkresult, desc.ID, "")
			case "resource_format":
				const checkchars = `.:/\`
				format := string(*element.Format)
				if idx := strings.IndexAny(format, checkchars); idx > -1 {
					message = append(message, ogdat.CheckMessage{
						Type:  ogdat.Warning,
						OGDID: desc.ID,
						Text:  resourceno + fmt.Sprintf("Ungültiges Zeichen '%c' (Index %d)", format[idx], idx)})
				}
				lower := strings.ToLower(format)
				if format != lower {
					message = append(message, ogdat.CheckMessage{
						Type:  ogdat.Warning,
						OGDID: desc.ID,
						Text:  resourceno + "Format darf nur in Kleinbuchstaben angegeben werden"})
				}
			// ###################### OPTIONALE FELDER ######################
			case "resource_name":
				resname := element.Name
				if resname == nil {
					continue
				}
				if len(*resname) == 0 {
					message = append(message, ogdat.CheckMessage{
						Type:  ogdat.Info | ogdat.EmptyData,
						OGDID: desc.ID,
						Text:  resourceno + emptystring})
					continue
				}
				if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*resname); !ok {
					if cerr, ok := err.(*ogdat.CheckInfo); ok {
						message = append(message, ogdat.CheckMessage{
							Type:  cerr.Status,
							OGDID: desc.ID,
							Text:  resourceno + fmt.Sprintf(invalidchars, cerr.Position, cerr)})
					}
				}
			case "resource_created":
				created := element.Created
				if created == nil {
					continue
				}
				if len(created.Raw) == 0 {
					message = append(message, ogdat.CheckMessage{
						Type:  ogdat.Info | ogdat.EmptyData,
						OGDID: desc.ID,
						Text:  resourceno + emptystring})
					continue
				}
				if created.Format != ogdat.CustomTimeSpecifier2 {
					message = append(message, ogdat.CheckMessage{
						Type:  ogdat.Error,
						OGDID: desc.ID,
						Text:  resourceno + fmt.Sprintf(wrongtimevalueCT2, created.Raw)})

				}
			case "resource_lastmodified":
				modified := element.LastModified
				if modified == nil {
					continue
				}
				if len(modified.Raw) == 0 {
					message = append(message, ogdat.CheckMessage{
						Type:  ogdat.Info | ogdat.EmptyData,
						OGDID: desc.ID,
						Text:  resourceno + emptystring})
					continue
				}
				if modified.Format != ogdat.CustomTimeSpecifier2 {
					message = append(message, ogdat.CheckMessage{
						Type:  ogdat.Error,
						OGDID: desc.ID,
						Text:  resourceno + fmt.Sprintf(wrongtimevalueCT2, modified.Raw)})
				}
			case "resource_size":
				size := element.Size
				if size == nil {
					continue
				}
				if len(*size) == 0 {
					message = append(message, ogdat.CheckMessage{
						Type:  ogdat.Info | ogdat.EmptyData,
						OGDID: desc.ID,
						Text:  resourceno + emptystring})
					continue
				}
				if _, err := strconv.Atoi(*size); err != nil {
					message = append(message, ogdat.CheckMessage{
						Type:  ogdat.Error,
						OGDID: desc.ID,
						Text:  resourceno + fmt.Sprintf("Nur Zahlenangaben erlaubt, Zeichenkette enthält aber nicht-Zahlenzeichen: '%s'", *size)})
				}
			case "resource_language":
				lang := element.Language
				if lang == nil {
					continue
				}
				if len(*lang) == 0 {
					message = append(message, ogdat.CheckMessage{
						Type:  ogdat.Info | ogdat.EmptyData,
						OGDID: desc.ID,
						Text:  resourceno + emptystring})
					continue
				}
				if !ogdat.CheckISOLanguage(*lang) {
					message = append(message, ogdat.CheckMessage{
						Type:  ogdat.Error,
						OGDID: desc.ID,
						Text:  resourceno + fmt.Sprintf("'%s' ist kein gültiger dreistelliger Sprachcode nach  ISO 639-2", *lang)})
				}
			case "resource_encoding":
				resencoding := element.Encoding
				if resencoding == nil {
					continue
				}
				if len(*resencoding) == 0 {
					message = append(message, ogdat.CheckMessage{
						Type:  ogdat.Info | ogdat.EmptyData,
						OGDID: desc.ID,
						Text:  resourceno + emptystring})
					continue
				}
				// the specification mentions only these encodings as valid
				if ogdat.CheckEncodingString(*resencoding, []string{"utf8", "utf16", "utf32"}) {
					continue
				}
				// ... but this is unfortunate, as certainly more encodings may be valid for OGD AT
				if ogdat.CheckIANAEncoding(*resencoding) {
					message = append(message, ogdat.CheckMessage{
						Type:  ogdat.Warning,
						OGDID: desc.ID,
						Text:  resourceno + fmt.Sprintf("'%s' ist kein gültiges Encoding nach Spezifiaktion, aber registiert bei IANA", *resencoding)})
					continue
				}
				// unknown encoding, report it
				message = append(message, ogdat.CheckMessage{
					Type:  ogdat.Error,
					OGDID: desc.ID,
					Text:  resourceno + fmt.Sprintf("'%s' ist kein bekanntes Encoding für Daten", *resencoding)})
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
				fdef := reflect.TypeOf(md).Elem().Field(i)
				if ogdat.GetIDFromMetaDataStructField(fdef) == elm.ID {
					fval := reflect.ValueOf(*md).Field(i)
					if fval.Kind() == reflect.Ptr && fval.IsNil() {
						message = append(message, ogdat.CheckMessage{Type: ogdat.Error, OGDID: elm.ID, Text: pflichtfeldfehlt})
						continue nextbeschreibung // required field is not present - nothing more to check
					}
				}
			}
			ielements = reflect.TypeOf(md.Extras).NumField()
			for i := 0; i < ielements; i++ {
				fdef := reflect.TypeOf(md.Extras).Field(i)
				if ogdat.GetIDFromMetaDataStructField(fdef) == elm.ID {
					fval := reflect.ValueOf((*md).Extras).Field(i)
					if fval.Kind() == reflect.Ptr && fval.IsNil() {
						message = append(message, ogdat.CheckMessage{Type: ogdat.Error, OGDID: elm.ID, Text: pflichtfeldfehlt})
						continue nextbeschreibung // required field is not present - nothing more to check
					}
				}
			}
		}

		switch elm.OGD_Kurzname {
		case "metadata_identifier":
			if md.Extras.Metadata_Identifier != nil && md.Extras.Metadata_Identifier.UUID == nil {
				message = append(message, ogdat.CheckMessage{
					Type:  ogdat.Error,
					OGDID: elm.ID,
					Text:  fmt.Sprintf("Feldwert vom Typ UUID erwartet, Wert ist aber keine UUID: '%s'", md.Extras.Metadata_Identifier.Raw)})
			}
		case "metadata_modified":
			if md.Extras.Metadata_Modified != nil && md.Extras.Metadata_Modified.Format != ogdat.CustomTimeSpecifier2 {
				message = append(message, ogdat.CheckMessage{
					Type:  ogdat.Error,
					OGDID: elm.ID,
					Text:  fmt.Sprintf(wrongtimevalueCT2, md.Extras.Metadata_Modified.Raw)})
			}
		case "title":
			if md.Title == nil {
				continue
			}
			// should this tool also check for spelling mistakes?
			// Don't think so, it does only check for adherence to the specification
			if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*md.Title); !ok {
				if cerr, ok := err.(*ogdat.CheckInfo); ok {
					message = append(message, ogdat.CheckMessage{
						Type:  cerr.Status,
						OGDID: elm.ID,
						Text:  fmt.Sprintf(invalidchars, cerr.Position, cerr)})
				}
			}
		case "description":
			if md.Description == nil {
				continue
			}
			if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*md.Description); !ok {
				if cerr, ok := err.(*ogdat.CheckInfo); ok {
					message = append(message, ogdat.CheckMessage{
						Type:  cerr.Status,
						OGDID: elm.ID,
						Text:  fmt.Sprintf(invalidchars, cerr.Position, cerr)})
				}
			}
		case "categorization":
			if cat := md.Extras.Categorization; cat == nil || len(cat.Kategorie) == 0 {
				message = append(message, ogdat.CheckMessage{
					Type:  ogdat.Warning,
					OGDID: elm.ID,
					Text:  "Die Kategorisierung darf zwar mit Kardinalität 'N' optional auftreten, jedoch sollte zumindest eine Zuordnung getroffen werden"})
			} else {
				if cat.IsString {
					message = append(message, ogdat.CheckMessage{
						Type:  ogdat.Info | ogdat.StructuralError,
						OGDID: elm.ID,
						Text:  "Kategorisierung muss als Array übergeben werden, ist aber als string spezifiziert"})

				}
				for _, element := range cat.Kategorie {
					if element.NumID == -1 {
						message = append(message, ogdat.CheckMessage{
							Type:  ogdat.Error,
							OGDID: elm.ID,
							Text:  fmt.Sprintf("Die Kategorie '%s' ist keine normierte OGD-Kategorie", element.ID)})
					}
				}
			}
		case "keywords":
			if keywords := md.Schlagworte; keywords == nil || len(keywords) == 0 {
				message = append(message, ogdat.CheckMessage{
					Type:  ogdat.Warning,
					OGDID: elm.ID,
					Text:  "Schlagworte dürfen zwar mit Kardinalität 'N' optional auftreten, die Angabe von Schlagwörtern wäre aber wünschenswert"})

			}
		case "maintainer":
			if md.Maintainer == nil {
				continue
			}
			if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*md.Maintainer); !ok {
				if cerr, ok := err.(*ogdat.CheckInfo); ok {
					message = append(message, ogdat.CheckMessage{
						Type:  cerr.Status,
						OGDID: elm.ID,
						Text:  fmt.Sprintf(invalidchars, cerr.Position, cerr)})
				}
			}
		case "license":
			if md.License == nil {
				continue
			}
			if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*md.License); !ok {
				if cerr, ok := err.(*ogdat.CheckInfo); ok {
					message = append(message, ogdat.CheckMessage{
						Type:  cerr.Status,
						OGDID: elm.ID,
						Text:  fmt.Sprintf(invalidchars, cerr.Position, cerr)})
				}
			}
		case "begin_datetime":
			if md.Extras.Begin_DateTime != nil && md.Extras.Begin_DateTime.Format != ogdat.CustomTimeSpecifier1 {
				message = append(message, ogdat.CheckMessage{
					Type:  ogdat.Error,
					OGDID: elm.ID,
					Text:  fmt.Sprintf(wrongtimevalueCT1, md.Extras.Begin_DateTime.Raw)})
			}
			// ###################### OPTIONALE FELDER ######################
		case "schema_name":
			schemaname := md.Extras.Schema_Name
			if schemaname == nil {
				continue
			}
			if len(*schemaname) == 0 {
				message = append(message, ogdat.CheckMessage{
					Type:  ogdat.Info | ogdat.EmptyData,
					OGDID: elm.ID,
					Text:  emptystring})
				continue
			}
			if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*schemaname); !ok {
				if cerr, ok := err.(*ogdat.CheckInfo); ok {
					message = append(message, ogdat.CheckMessage{
						Type:  cerr.Status,
						OGDID: elm.ID,
						Text:  fmt.Sprintf(invalidchars, cerr.Position, cerr)})
				}
				var ogdschemaspec = []string{Version, "2.2"}
				for _, val := range ogdschemaspec {
					if strings.Contains(*schemaname, val) {
						continue nextbeschreibung
					}
				}
				message = append(message, ogdat.CheckMessage{
					Type:  ogdat.Info,
					OGDID: elm.ID,
					Text:  fmt.Sprintf("Schemabezeichnung vorhanden, enthält keine Referenz auf Version 2.2: '%s'", *schemaname)})
			}
		case "schema_language":
			lang := md.Extras.Schema_Language
			if lang == nil {
				continue
			}
			if len(*lang) == 0 {
				message = append(message, ogdat.CheckMessage{
					Type:  ogdat.Info | ogdat.EmptyData,
					OGDID: elm.ID,
					Text:  emptystring})
				continue
			}
			const ogdschemalanguage = "ger"
			if ogdschemalanguage != strings.ToLower(*lang) {
				message = append(message, ogdat.CheckMessage{
					Type:  ogdat.Error,
					OGDID: elm.ID,
					Text:  fmt.Sprintf("Schemasprache als '%s' erwartet, der Wert ist aber '%s'", ogdschemalanguage, *lang)})
			}
		case "schema_characterset":
			charset := md.Extras.Schema_Characterset
			if charset == nil {
				continue
			}
			if len(*charset) == 0 {
				message = append(message, ogdat.CheckMessage{
					Type:  ogdat.Info | ogdat.EmptyData,
					OGDID: elm.ID,
					Text:  emptystring})
				continue
			}
			const ogdschemacharacterset = "utf8"
			if !ogdat.CheckEncodingString(*charset, []string{"utf8"}) {
				message = append(message, ogdat.CheckMessage{
					Type:  ogdat.Error,
					OGDID: elm.ID,
					Text:  fmt.Sprintf("Characterset des Schemas als '%s' erwartet, der Wert ist aber '%s'", ogdschemacharacterset, *charset)})
			}
		case "metadata_linkage":
			linkage := md.Extras.Metadata_Linkage
			if linkage == nil {
				continue
			}
			if !linkage.IsArray {
				message = append(message, ogdat.CheckMessage{
					Type:  ogdat.Info | ogdat.StructuralError,
					OGDID: elm.ID,
					Text:  fmt.Sprintf("JSON vom Typ 'Array of String' erwartet, es wurde jedoch ein einzelner Wert geliefert")})
			}
			for _, element := range linkage.Url {
				if element.URL == nil {
					message = append(message, ogdat.CheckMessage{
						Type:  ogdat.Error,
						OGDID: elm.ID,
						Text:  fmt.Sprintf(expectedlink, element.Raw)})
				} else {
					_, checkresult := ogdat.CheckUrl(element.Raw, followhttplinks)
					message = ogdat.AppendcheckerrorTocheckmessage(message, checkresult, elm.ID, "")
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
					Type:  ogdat.Warning,
					OGDID: elm.ID,
					Text:  fmt.Sprintf("Beschreibung enthält weniger als %d Zeichen (sinnvolle Beschreibung?)", ogddesclen)})

			}
			if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*desc); !ok {
				if cerr, ok := err.(*ogdat.CheckInfo); ok {
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
					Type:  ogdat.Error,
					OGDID: elm.ID,
					Text:  fmt.Sprintf(expectedlink, link.Raw)})
			} else {
				_, checkresult := ogdat.CheckUrl(link.Raw, followhttplinks)
				message = ogdat.AppendcheckerrorTocheckmessage(message, checkresult, elm.ID, "")
			}
		case "publisher":
			publisher := md.Extras.Publisher
			if publisher == nil {
				continue
			}
			if len(*publisher) == 0 {
				message = append(message, ogdat.CheckMessage{
					Type:  ogdat.Info | ogdat.EmptyData,
					OGDID: elm.ID,
					Text:  emptystring})
				continue
			}
			if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*publisher); !ok {
				if cerr, ok := err.(*ogdat.CheckInfo); ok {
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
			if len(*toponym) == 0 {
				message = append(message, ogdat.CheckMessage{
					Type:  ogdat.Info | ogdat.EmptyData,
					OGDID: elm.ID,
					Text:  emptystring})
				continue
			}
			if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*toponym); !ok {
				if cerr, ok := err.(*ogdat.CheckInfo); ok {
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
			if len(*bbox) == 0 {
				message = append(message, ogdat.CheckMessage{
					Type:  ogdat.Info | ogdat.EmptyData,
					OGDID: elm.ID,
					Text:  emptystring})
				continue
			}
			if ok, err := checkOGDBBox(*bbox); !ok {
				message = append(message, ogdat.CheckMessage{
					Type:  ogdat.Error,
					OGDID: elm.ID,
					Text:  fmt.Sprintf("Zeichenfolge enthält keinen gültigen WKT für die örtliche Begrenzung (Boundingbox): %s", err)})
			}
		case "end_datetime":
			endtime := md.Extras.End_DateTime
			if endtime == nil {
				continue
			}
			if len(endtime.Raw) == 0 {
				message = append(message, ogdat.CheckMessage{
					Type:  ogdat.Info | ogdat.EmptyData,
					OGDID: elm.ID,
					Text:  emptystring})
				continue
			}
			if endtime.Format != ogdat.CustomTimeSpecifier1 {
				message = append(message, ogdat.CheckMessage{
					Type:  ogdat.Error,
					OGDID: elm.ID,
					Text:  fmt.Sprintf(wrongtimevalueCT1, endtime.Raw)})
			}
		case "update_frequency":
			frequency := md.Extras.Update_Frequency
			if frequency == nil {
				continue
			}
			if len(frequency.Raw) == 0 {
				message = append(message, ogdat.CheckMessage{
					Type:  ogdat.Info | ogdat.EmptyData,
					OGDID: elm.ID,
					Text:  emptystring})
				continue
			}
			if frequency.NumID == -1 {
				message = append(message, ogdat.CheckMessage{
					Type:  ogdat.Warning,
					OGDID: elm.ID,
					Text:  fmt.Sprintf("Feldwert in Anlehnung an ON/EN/ISO 19115:2003 erwartet (gültige Werte sind in der OGD Spezifikation definiert), Wert entspricht aber nicht diesem Typ: '%s'", frequency.Raw)})
			}
		case "lineage_quality":
			quality := md.Extras.Lineage_Quality
			if quality == nil {
				continue
			}
			if len(*quality) == 0 {
				message = append(message, ogdat.CheckMessage{
					Type:  ogdat.Info | ogdat.EmptyData,
					OGDID: elm.ID,
					Text:  emptystring})
				continue
			}
			if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*quality); !ok {
				if cerr, ok := err.(*ogdat.CheckInfo); ok {
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
			if len(*en_desc) == 0 {
				message = append(message, ogdat.CheckMessage{
					Type:  ogdat.Info | ogdat.EmptyData,
					OGDID: elm.ID,
					Text:  emptystring})
				continue
			}
			if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*en_desc); !ok {
				if cerr, ok := err.(*ogdat.CheckInfo); ok {
					message = append(message, ogdat.CheckMessage{
						Type:  cerr.Status,
						OGDID: elm.ID,
						Text:  fmt.Sprintf(invalidchars, cerr.Position, cerr)})
				}
			}
		case "license_citation":
			licensecit := md.Extras.License_Citation
			if licensecit == nil {
				continue
			}
			if len(*licensecit) == 0 {
				message = append(message, ogdat.CheckMessage{
					Type:  ogdat.Info | ogdat.EmptyData,
					OGDID: elm.ID,
					Text:  emptystring})
				continue
			}
			if ok, err := ogdat.CheckOGDTextStringForSaneCharacters(*licensecit); !ok {
				if cerr, ok := err.(*ogdat.CheckInfo); ok {
					message = append(message, ogdat.CheckMessage{
						Type:  cerr.Status,
						OGDID: elm.ID,
						Text:  fmt.Sprintf(invalidchars, cerr.Position, cerr)})
				}
			}
		// neu ab version 2.2
		case "metadata_original_portal":
			mdorigportal := md.Extras.Metadata_OriginalPortal
			if mdorigportal == nil {
				continue
			}
			if mdorigportal.URL == nil {
				message = append(message, ogdat.CheckMessage{
					Type:  ogdat.Error,
					OGDID: elm.ID,
					Text:  fmt.Sprintf(expectedlink, mdorigportal.Raw)})
			} else {
				_, checkresult := ogdat.CheckDataPortalUrl(mdorigportal.Raw, followhttplinks)
				message = ogdat.AppendcheckerrorTocheckmessage(message, checkresult, elm.ID, "")
			}
		case "maintainer_email":
			maintainermail := md.Maintainer_Email
			if maintainermail == nil {
				continue
			}
			if maintainermail.URL == nil {
				message = append(message, ogdat.CheckMessage{
					Type:  ogdat.Error,
					OGDID: elm.ID,
					Text:  fmt.Sprintf(expectedlink, maintainermail.Raw)})
			} else {
				_, checkresult := ogdat.CheckUrl(maintainermail.Raw, followhttplinks)
				message = ogdat.AppendcheckerrorTocheckmessage(message, checkresult, elm.ID, "")
			}
		}
	}
	return
}
