package ogdat

import (
	"code.google.com/p/go-uuid/uuid"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

/*	Version10 = "OGD Austria Metadata 1.0" // Version 1.0: 24.10.2011
	Version11 = "OGD Austria Metadata 1.1" // Version 1.1: 12.03.2012
	Version20 = "OGD Austria Metadata 2.0" // Version 2.0: 10.10.2012
	Version21 = "OGD Austria Metadata 2.1" // Version 2.1: 15.10.2012
	Version22 = "OGD Austria Metadata 2.2" // Version 2.1: 12.12.2013
*/

const CustomTimeSpecifier1 = "2006-01-02T15:04:05" // RFC 3339 = ISO 8601 ohne Zeitzone
const CustomTimeSpecifier2 = "2006-01-02"          // RFC 3339 = ISO 8601 ohne Zeitzone

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

var Categories = []Kategorie{
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

var Categorymap = make(map[string]Kategorie)

type Tags string
type ResourceSpecifier string

type Cycle struct {
	NumID                       int
	DomainCode                  string
	MD_MaintenanceFrequencyCode string
	Name_DE                     string
	Raw                         string
}

var (
	CycCont     = Cycle{NumID: 1, DomainCode: "001", MD_MaintenanceFrequencyCode: "continual", Name_DE: "kontinuierlich"}
	CycDaily    = Cycle{NumID: 2, DomainCode: "002", MD_MaintenanceFrequencyCode: "daily", Name_DE: "täglich"}
	CycWeekly   = Cycle{NumID: 3, DomainCode: "003", MD_MaintenanceFrequencyCode: "weekly", Name_DE: "wöchentlich"}
	CycFortNly  = Cycle{NumID: 4, DomainCode: "004", MD_MaintenanceFrequencyCode: "fortnightly", Name_DE: "14-tägig"}
	CycMonthly  = Cycle{NumID: 5, DomainCode: "005", MD_MaintenanceFrequencyCode: "monthly", Name_DE: "monatlich"}
	CycQuart    = Cycle{NumID: 6, DomainCode: "006", MD_MaintenanceFrequencyCode: "quarterly", Name_DE: "quartalsweise"}
	CycBiAnn    = Cycle{NumID: 7, DomainCode: "007", MD_MaintenanceFrequencyCode: "biannually", Name_DE: "halbjährlich"}
	CycAnnually = Cycle{NumID: 8, DomainCode: "008", MD_MaintenanceFrequencyCode: "annually", Name_DE: "jährlich"}
	CycNeeded   = Cycle{NumID: 9, DomainCode: "009", MD_MaintenanceFrequencyCode: "asNeeded", Name_DE: "nach Bedarf"}
	CycIrreg    = Cycle{NumID: 10, DomainCode: "010", MD_MaintenanceFrequencyCode: "irregular", Name_DE: "unregelmäßig"}
	CycNP       = Cycle{NumID: 11, DomainCode: "011", MD_MaintenanceFrequencyCode: "notPlanned", Name_DE: "nicht geplant"}
	CycUnknown  = Cycle{NumID: 12, DomainCode: "012", MD_MaintenanceFrequencyCode: "unknown", Name_DE: "unbekannt"}
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

type MetaDataLinkage struct {
	Url     []Url
	IsArray bool
}

type MetaDataKategorie struct {
	Kategorie []Kategorie
	IsString  bool
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
		if found = cmpstrtocycle(raw, matchcyc); found == true {
			break
		}
	}

	if found {
		*cyc = cycles[idx]
	} else {
		cyc.NumID = -1
		cyc.Raw = raw
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

func (u *MetaDataLinkage) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && data[0] != '[' {
		var inu Url
		e := json.Unmarshal(data, &inu)
		if e != nil {
			return e
		}
		u.Url = append(u.Url, inu)
		return nil
	}
	u.IsArray = true
	return json.Unmarshal(data, (*[]Url)(&u.Url))
}

func (id *Identifier) UnmarshalJSON(data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	id.Raw = raw
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

	corecat, found := Categorymap[raw]
	if !found {
		kat.NumID = -1
		kat.ID = raw
		kat.PrettyName = "**** NON core category **** - " + kat.ID
	} else {
		*kat = corecat
	}
	return nil
}

func (kat *MetaDataKategorie) UnmarshalJSON(data []byte) error {
	if len(data) > 1 {
		// Some specify the category as an embedded array ...
		if data[0] == '"' && data[1] == '[' {
			kat.IsString = true
			var embeddedarray string
			e := json.Unmarshal(data, &embeddedarray)
			if e != nil {
				return e
			}
			data = []byte(embeddedarray)
		}
		// .. that's how it should be ...
		if data[0] == '[' {
			return json.Unmarshal(data, (*[]Kategorie)(&kat.Kategorie))
		}
		// and some only specify a single string
		if data[0] == '"' {
			kat.IsString = true
			var cat Kategorie
			e := json.Unmarshal(data, &cat)
			if e != nil {
				return e
			}
			kat.Kategorie = append(kat.Kategorie, cat)
			return nil
		}
	}
	return errors.New("MetaDataKategorie: Unknow structure to unmarshal")
}

var specification = make(map[string]*OGDSet)

type Occurrence int

const (
	OccUndef Occurrence = iota
	OccRequired
	OccOptional
)

type Beschreibung struct {
	ID            int
	Bezeichner    string
	OGD_Kurzname  string
	CKAN_Feld     string
	Anzahl        string
	Definition_DE string
	Erlauterung   string
	Beispiel      string
	ONA2270       string
	ISO19115      string
	RDFProperty   string
	Definition_EN string
	occurrence    Occurrence
	version       string
}

func NewBeschreibung(ID int, occur Occurrence, ver string) *Beschreibung {
	return &Beschreibung{ID: ID, occurrence: occur, version: ver}
}

func (desc *Beschreibung) Version() string {
	return desc.version
}

func (desc *Beschreibung) Occurrence() Occurrence {
	return desc.occurrence
}

func (desc *Beschreibung) IsRequired() bool {
	return desc.occurrence == OccRequired
}

type OGDSet struct {
	Label        []string
	Beschreibung []*Beschreibung
}

func (set *OGDSet) GetBeschreibungForID(id int) (*Beschreibung, string) {
	if set != nil {
		for idx, elm := range set.Beschreibung {
			if elm.ID == id {
				return set.Beschreibung[idx], set.Beschreibung[idx].OGD_Kurzname
			}
		}
	}
	return nil, ""
}

func RegisterFromCSVFile(version, specfile string) *OGDSet {
	set, _ := Loadogdatspec(version, specfile)
	specification[version] = set
	return set
}

func Register(version string, set *OGDSet) *OGDSet {
	specification[version] = set
	return set
}

func GetOGDSetForVersion(version string) *OGDSet {
	return specification[version]
}

func GetIDFromMetaDataStructField(val reflect.StructField) int {
	ids := val.Tag.Get("ogdat")
	if idx := strings.Index(ids, "ID"); idx > -1 {
		ids = ids[idx+len("ID")+1:]
		if idx = strings.IndexRune(ids, ','); idx > -1 {
			ids = ids[:idx]
		}
		if i, err := strconv.Atoi(ids); err == nil {
			return i
		}
	}
	return -1
}

type Extras struct {
	Metadata_Identifier *string            `json:"metadata_identifier"`
	Schema_Name         *string            `json:"schema_name"`
	Maintainer_Link     *string            `json:"maintainer_link"`
	Publisher           *string            `json:"publisher"`
	Geographic_BBox     *string            `json:"geographic_bbox"`
	Geographich_Toponym *string            `json:"geographic_toponym"`
	Categorization      *MetaDataKategorie `json:"categorization"`
}

type MinimalMetaData struct {
	Description *string `json:"notes"`
	Extras      `json:"extras"`
}

type Metadater interface {
	Check(bool) ([]CheckMessage, error)
	MinimalMetadata() *MinimalMetaData
}

func MinimalMetaDataforJSONStream(jsondata io.Reader) (*MinimalMetaData, error) {
	bytedata, err := ioutil.ReadAll(jsondata)
	if err != nil {
		return nil, err
	}

	data := &MinimalMetaData{}
	if err := json.Unmarshal(bytedata, data); err != nil {
		if len(bytedata) > 0 && bytedata[0] == '"' {
			return nil, nil
		}
		return nil, err
	}
	return data, nil
}

const versionextractorregexp = `(\d+(?:\.\d+)*)`

func OGDVersionfromString(match string) (version string) {
	r := regexp.MustCompile(versionextractorregexp)
	s := r.FindAllString(match, 2)
	if len(s) == 1 {
		return s[0]
	}
	return
}

func init() {
	for idx, val := range Categories {
		Categorymap[val.ID] = Categories[idx]
	}
}
