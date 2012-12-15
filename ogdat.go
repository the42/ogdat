package ogdat

import (
	"code.google.com/p/go-uuid/uuid"
	"encoding/json"
	"net/url"
	"strconv"
	"time"
)

const OGDTimeSpecifier = "2006-01-02T15:04:05" // RFC 3339 = ISO 8601 ohne Zeitzone
const (
	OGDTime2 = time.RFC3339Nano
	OGDTime3 = time.RFC3339
	OGDTime1 = OGDTimeSpecifier
	OGDTimeUnknow
)

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

type Identfier struct {
	*uuid.UUID
	Raw string
}

func (id *Identfier) String() string {
	return id.Raw
}

type Time struct {
	*time.Time
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

	ogdtime.Format = OGDTime1
	t, err := time.Parse(ogdtime.Format, raw)
	if err != nil {
		ogdtime.Format = OGDTime2
		t, err = time.Parse(ogdtime.Format, raw)
		if err != nil {
			ogdtime.Format = OGDTime3
			t, err = time.Parse(ogdtime.Format, raw)
			if err != nil {
				ogdtime.Format = OGDTimeUnknow
			}
		}
	}
	ogdtime.Time = &t
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

func (id *Identfier) UnmarshalJSON(data []byte) error {
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

type Extras struct {
	// Core
	Metadata_Identifier Identfier   `json:"metadata_identifier"` // CKAN uses since API Version 2 a UUID V4, cf. https://github.com/okfn/ckan/blob/master/ckan/model/types.py
	Metadata_Modified   *Time       `json:"metadata_modified"`
	Categorization      []Kategorie `json:"categorization"`
	Begin_DateTime      *Time       `json:"begin_datetime"`

	// Optional
	Schema_Name           *string `json:"schema_name"`
	Schema_Language       *string `json:"schema_language"`     // always "ger"
	Schema_Characterset   *string `json:"schema_characterset"` // always "utf8", cf. https://www.ghrsst.org/files/download.php?m=documents&f=ISO%2019115%20.pdf
	MetaData_Linkage      []Url   `json:"metadata_linkage"`
	Attribute_Description *string `json:"attribute_description"`
	Maintainer_Link       *Url    `json:"maintainer_link"`
	Publisher             *string `json:"publisher"`
	Geographich_Toponym   *string `json:"geographic_toponym"`

	/*  ON/EN/ISO 19115:2003: westBL (344) & eastBL (345) & southBL (346) & northBL (347)
	 * TODO: Specifiaction says a WKT of POLYGON should be used, which would make a
	 * POLYGON ((-180.00 -90.00, 180.00 90.00)) but Example states
	 * POLYGON (-180.00 -90.00, 180.00 90.00)
	 * UNDER CLARIFICATION
	 */
	Geographic_BBox  *string `json:"geographic_bbox"`
	End_DateTime     *Time   `json:"end_datetime"`
	Update_Frequency *Cycle  `json:"update_frequency"`
	Lineage_Quality  *string `json:"lineage_quality"`
	EnTitleDesc      *string `json:"en_title_and_desc"`
}

type Resource struct {
	// Core
	URL    *Url              `json:"url"`
	Format ResourceSpecifier `json:"format"`

	// Optional
	Name         *string `json:"name"`
	Created      *Time   `json:"created"`
	LastModified *Time   `json:"last_modified"`

	/*
	 * dcat:bytes a rdf:Property, owl:DatatypeProperty;
	 * rdfs:isDefinedBy <http://www.w3.org/ns/dcat>;
	 * rdfs:label "size in bytes";
	 * rdfs:comment "describe size of resource in bytes";
	 * rdfs:domain dcat:Distribution;
	 * rdfs:range xsd:integer .
	 */
	Size             *string `json:"size"`
	License_Citation *string `json:"license_citation"`
	Language         *string `json:"language"`
	/* Here we have a problem in spec 2.1. which says "nach ISO\IEC 10646-1", which means utf-8, utf-16 and utf-32.
	 * We would certainly support more encodings, as eg.
	 * ISO 19115 / B.5.10 MD_CharacterSetCode<> or
	 * http://www.iana.org/assignments/character-sets/character-sets.xml
	 */
	Encoding *string `json:"characterset"`
}

type MetaData struct {
	// Core
	Title       string `json:"title"`
	Description string `json:"notes"`
	Schlagworte []Tags `json:"tags"`
	Maintainer  string `json:"maintainer"`
	License     string `json:"license"` // Sollte URI des Lizenzdokuments sein

	// nested structs
	Extras   `json:"extras"`
	Resource []Resource `json:"resources"`
}

func init() {
	for _, val := range categories {
		categorymap[val.ID] = val
	}
}
