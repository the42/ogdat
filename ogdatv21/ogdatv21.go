package ogdatv21

import (
	"code.google.com/p/go-uuid/uuid"
	"encoding/json"
	"github.com/the42/ogdat"
	"net/url"
	"reflect"
	"strconv"
	"time"
)

const Version = "OGD Austria Metadata 2.1" // Version 2.1: 15.10.2012
const specfile = "ogdat_spec-2.1.csv"
const CustomTimeSpecifier1 = "2006-01-02T15:04:05" // RFC 3339 = ISO 8601 ohne Zeitzone
const CustomTimeSpecifier2 = "2006-12-02"          // RFC 3339 = ISO 8601 ohne Zeitzone

/// BEGIN:check wheater this code may be factored out
var TimeFormat = []string{
	time.RFC3339Nano,
	time.RFC3339,
	CustomTimeSpecifier1,
	CustomTimeSpecifier2,
}

type Kategorie struct {
	NumID       int `json:"-"`
	ID          string
	PrettyName  string `json:"-"`
	RDFProperty string `json:"-"`
}

func (kat *Kategorie) String() string {
	return kat.PrettyName
}

var (
	Arbeit           = Kategorie{NumID: 1, ID: "arbeit", PrettyName: "Arbeit", RDFProperty: ""}
	Bevoelkerung     = Kategorie{NumID: 2, ID: "bevölkerung", PrettyName: "Bevölkerung", RDFProperty: ""}
	BildungForschung = Kategorie{NumID: 3, ID: "bildung-und-forschung", PrettyName: "Bildung und Forschung", RDFProperty: ""}
	FinanzRW         = Kategorie{NumID: 4, ID: "finanzen-und-rechnungswesen", PrettyName: "Finanzen und Rechnungswesen", RDFProperty: ""}
	GeographPlanung  = Kategorie{NumID: 5, ID: "geographie-und-planung", PrettyName: "Geographie und Planung", RDFProperty: ""}
	GesellSoziales   = Kategorie{NumID: 6, ID: "gesellschaft-und-soziales", PrettyName: "Gesellschaft und Soziales", RDFProperty: ""}
	Gesundheit       = Kategorie{NumID: 7, ID: "gesundheit", PrettyName: "Gesundheit", RDFProperty: ""}
	KunstKultur      = Kategorie{NumID: 8, ID: "kunst-und-kultur", PrettyName: "Kunst und Kultur", RDFProperty: ""}
	LandFW           = Kategorie{NumID: 9, ID: "land-und-forstwirtschaft", PrettyName: "Land und Forstwirtschaft", RDFProperty: ""}
	SportFZ          = Kategorie{NumID: 10, ID: "sport-und-freizeit", PrettyName: "Sport und Freizeit", RDFProperty: ""}
	Umwelt           = Kategorie{NumID: 11, ID: "umwelt", PrettyName: "Umwelt", RDFProperty: ""}
	VerkehrTechnik   = Kategorie{NumID: 12, ID: "verkehr-und-technik", PrettyName: "Verkehr und Technik", RDFProperty: ""}
	VerwaltPol       = Kategorie{NumID: 13, ID: "verwaltung-und-politik", PrettyName: "Verwaltung und Politik", RDFProperty: ""}
	WirtTourism      = Kategorie{NumID: 14, ID: "wirtschaft-und-tourismus", PrettyName: "Wirtschaft und Tourismus", RDFProperty: ""}
)

var categories = []Kategorie{
	Arbeit,
	Bevoelkerung,
	BildungForschung,
	FinanzRW,
	GeographPlanung,
	GesellSoziales,
	Gesundheit,
	KunstKultur,
	LandFW,
	SportFZ,
	Umwelt,
	VerkehrTechnik,
	VerwaltPol,
	WirtTourism,
}

var categorymap = make(map[string]Kategorie)

type Tags string
type ResourceSpecifier string

type Cycle struct {
	NumID                       int
	DomainCode                  string
	MD_MaintenanceFrequencyCode string
	Name_DE                     string
}

var (
	CycCont     = Cycle{1, "001", "continual", "kontinuierlich"}
	CycDaily    = Cycle{2, "002", "daily", "täglich"}
	CycWeekly   = Cycle{3, "003", "weekly", "wöchentlich"}
	CycFortNly  = Cycle{4, "004", "fortnightly", "14-tägig"}
	CycMonthly  = Cycle{5, "005", "monthly", "monatlich"}
	CycQuart    = Cycle{6, "006", "quarterly", "quartalsweise"}
	CycBiAnn    = Cycle{7, "007", "biannually", "halbjährlich"}
	CycAnnually = Cycle{8, "008", "annually", "jährlich"}
	CycNeeded   = Cycle{9, "009", "asNeeded", "nach Bedarf"}
	CycIrreg    = Cycle{10, "010", "irregular", "unregelmäßig"}
	CycNP       = Cycle{11, "011", "notPlanned", "nicht geplant"}
	CycUnknown  = Cycle{12, "012", "unknown", "unbekannt"}
)

var cycles = []Cycle{
	CycCont,
	CycDaily,
	CycWeekly,
	CycFortNly,
	CycMonthly,
	CycQuart,
	CycBiAnn,
	CycAnnually,
	CycNeeded,
	CycIrreg,
	CycNP,
	CycUnknown,
}

type Url struct {
	*url.URL
	Raw string
}

type Identifier struct {
	*uuid.UUID
	Raw string
}

func (id *Identifier) String() string {
	return id.Raw
}

type Time struct {
	time.Time
	Raw    string
	Format string
}

func (time *Time) String() string {
	return time.Raw
}

func (cyc *Cycle) String() string {
	return cyc.Name_DE
}

func cmpstrtocycle(raw string, cyc Cycle) bool {
	if raw == cyc.Name_DE || raw == cyc.DomainCode || raw == cyc.MD_MaintenanceFrequencyCode {
		return true
	}
	if len(raw) > 0 {
		if i, err := strconv.Atoi(raw); err == nil && i == cyc.NumID {
			return true
		}
	}
	return false
}

func (cyc *Cycle) UnmarshalJSON(data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	var found bool
	var idx int
	var matchcyc Cycle

	for idx, matchcyc = range cycles {
		if found := cmpstrtocycle(raw, matchcyc); found == true {
			break
		}
	}

	if found {
		*cyc = cycles[idx]
	} else {
		cyc.NumID = -1
		cyc.Name_DE = "**** NON cycle spec **** - " + raw
		cyc.MD_MaintenanceFrequencyCode = cyc.Name_DE
	}
	return nil
}

func (ogdtime *Time) UnmarshalJSON(data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	ogdtime.Raw = raw

	for idx, val := range TimeFormat {
		t, err := time.Parse(val, raw)
		if err == nil {
			ogdtime.Format = TimeFormat[idx]
			ogdtime.Time = t
			break
		}
	}
	return nil
}

func (u *Url) UnmarshalJSON(data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	u.Raw = raw
	url, _ := url.Parse(raw) // an actuall error is not important. If url can not be parsed, result will be nil, which is fine here
	u.URL = url
	return nil
}

func (id *Identifier) UnmarshalJSON(data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	id.Raw = string(raw)
	if uuid := uuid.Parse(raw); uuid != nil {
		id.UUID = &uuid
	}
	return nil
}

func (kat *Kategorie) UnmarshalJSON(data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	corecat, found := categorymap[raw]
	if !found {
		kat.NumID = -1
		kat.ID = raw
		kat.PrettyName = "**** NON core category **** - " + kat.ID
	} else {
		*kat = corecat
	}
	return nil
}

/// END:check wheater this code may be factored out

type Extras struct {
	// Core
	Metadata_Identifier *Identifier `json:"metadata_identifier" ogdat:"ID:1"` // CKAN uses since API Version 2 a UUID V4, cf. https://github.com/okfn/ckan/blob/master/ckan/model/types.py
	Metadata_Modified   *Time       `json:"metadata_modified" ogdat:"ID:5"`
	Categorization      []Kategorie `json:"categorization" ogdat:"ID:10"`
	Begin_DateTime      *Time       `json:"begin_datetime" ogdat:"ID:24"`

	// Optional
	Schema_Name           *string `json:"schema_name" ogdat:"ID:2"`
	Schema_Language       *string `json:"schema_language" ogdat:"ID:3"`     // always "ger"
	Schema_Characterset   *string `json:"schema_characterset" ogdat:"ID:4"` // always "utf8", cf. https://www.ghrsst.org/files/download.php?m=documents&f=ISO%2019115%20.pdf
	MetaData_Linkage      []Url   `json:"metadata_linkage" ogdat:"ID:6"`
	Attribute_Description *string `json:"attribute_description" ogdat:"ID:12"`
	Maintainer_Link       *Url    `json:"maintainer_link" ogdat:"ID:13"`
	Publisher             *string `json:"publisher" ogdat:"ID:20"`
	Geographich_Toponym   *string `json:"geographic_toponym" ogdat:"ID:22"`

	/*  ON/EN/ISO 19115:2003: westBL (344) & eastBL (345) & southBL (346) & northBL (347)
	 * TODO: Specifiaction says a WKT of POLYGON should be used, which would make a
	 * POLYGON ((-180.00 -90.00, 180.00 90.00)) but Example states
	 * POLYGON (-180.00 -90.00, 180.00 90.00)
	 * UNDER CLARIFICATION
	 */
	Geographic_BBox  *string `json:"geographic_bbox" ogdat:"ID:23"`
	End_DateTime     *Time   `json:"end_datetime" ogdat:"ID:25"`
	Update_Frequency *Cycle  `json:"update_frequency" ogdat:"ID:26"`
	Lineage_Quality  *string `json:"lineage_quality" ogdat:"ID:27"`
	EnTitleDesc      *string `json:"en_title_and_desc" ogdat:"ID:28"`
}

type Resource struct {
	// Core
	URL    *Url               `json:"url" ogdat:"ID:14"`
	Format *ResourceSpecifier `json:"format" ogdat:"ID:15"`

	// Optional
	Name         *string `json:"name" ogdat:"ID:16"`
	Created      *Time   `json:"created" ogdat:"ID:17"`
	LastModified *Time   `json:"last_modified" ogdat:"ID:18"`

	/*
	 * dcat:bytes a rdf:Property, owl:DatatypeProperty;
	 * rdfs:isDefinedBy <http://www.w3.org/ns/dcat>;
	 * rdfs:label "size in bytes";
	 * rdfs:comment "describe size of resource in bytes";
	 * rdfs:domain dcat:Distribution;
	 * rdfs:range xsd:integer .
	 */
	Size             *string `json:"size" ogdat:"ID:29"`
	License_Citation *string `json:"license_citation" ogdat:"ID:30"`
	Language         *string `json:"language" ogdat:"ID:31"`
	/* Here we have a problem in spec 2.1. which says "nach ISO\IEC 10646-1", which means utf-8, utf-16 and utf-32.
	 * We would certainly support more encodings, as eg.
	 * ISO 19115 / B.5.10 MD_CharacterSetCode<> or
	 * http://www.iana.org/assignments/character-sets/character-sets.xml
	 */
	Encoding *string `json:"characterset" ogdat:"ID:32"`
}

type MetaData struct {
	// Core
	Title       *string `json:"title" ogdat:"ID:8"`
	Description *string `json:"notes" ogdat:"ID:9"`
	Schlagworte []Tags  `json:"tags" ogdat:"ID:11"`
	Maintainer  *string `json:"maintainer" ogdat:"ID:19"`
	License     *string `json:"license" ogdat:"ID:21"` // Sollte URI des Lizenzdokuments sein

	// nested structs
	Extras   `json:"extras"`
	Resource []Resource `json:"resources"`
}

func (md *MetaData) GetBeschreibungForFieldName(name string) *ogdat.Beschreibung {
	if f, ok := reflect.TypeOf(md).Elem().FieldByName(name); ok {
		if id := ogdat.GetIDFromMetaDataStructField(f); id > -1 {
			beschreibung, _ := ogdat.GetOGDSetForVersion(Version).GetBeschreibungForID(id)
			return beschreibung
		}
	}
	return nil
}

func init() {
	for idx, val := range categories {
		categorymap[val.ID] = categories[idx]
	}
	ogdat.RegisterFromCSVFile(Version, specfile)
}
