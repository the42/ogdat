package ogdat

import (
	"reflect"
	"strconv"
	"strings"
)

/*	Version10 = "OGD Austria Metadata 1.0" // Version 1.0: 24.10.2011
	Version11 = "OGD Austria Metadata 1.1" // Version 1.1: 12.03.2012
	Version20 = "OGD Austria Metadata 2.0" // Version 2.0: 10.10.2012
	Version21 = "OGD Austria Metadata 2.1" // Version 2.1: 15.10.2012
*/

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

// Return if a value is nil. If value is nor a pointer, return will be false
func IsNil(val interface{}) bool {
	v := reflect.ValueOf(val)
	if v.Kind() != reflect.Ptr {
		return false
	}
	return v.IsNil()
}
