package ogdat

import (
	"encoding/csv"
	"io"
	"log"
	"os"
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

type CheckMessage struct {
	Type    int // 1 = Info, 2 = Warning, 3 = Error
	Text    string
	OGDID   int
	Context string
}

type Checker interface {
	Check() []CheckMessage
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
				return set.Beschreibung[idx], set.Label[idx]
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

// Return if a value is nil. Will panic if the value is not a pointer
func MetaDataStructFieldIsNil(val interface{}) bool {
	v := reflect.ValueOf(val)
	if v.Kind() != reflect.Ptr {
		panic("Struct field is not a pointer")
	}
	return v.IsNil()
}

func Loadogdatspec(version, filename string) (*OGDSet, error) {
	reader, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	csvreader := csv.NewReader(reader)
	csvreader.Comma = '|'
	csvreader.LazyQuotes = true

	// Read the first line and use it as the labels for the items to load
	record, err := csvreader.Read()
	if err != nil {
		return nil, err
	}

	set := &OGDSet{Label: record}

	spec := make([]*Beschreibung, 0)
	for record, err = csvreader.Read(); err != io.EOF; record, err = csvreader.Read() {
		id, _ := strconv.Atoi(record[0])
		var occ Occurrence
		switch record[12][0] {
		case 'R':
			occ = OccRequired
		case 'O':
			occ = OccOptional
		}
		descrecord := NewBeschreibung(id, occ, version)

		descrecord.Bezeichner = record[1]
		descrecord.OGD_Kurzname = record[2]
		descrecord.CKAN_Feld = record[3]
		descrecord.Anzahl = record[4]
		descrecord.Definition_DE = record[5]
		descrecord.Erlauterung = record[6]
		descrecord.Beispiel = record[7]
		descrecord.ONA2270 = record[8]
		descrecord.ISO19115 = record[9]
		descrecord.RDFProperty = record[10]
		descrecord.Definition_EN = record[11]

		spec = append(spec, descrecord)
	}
	set.Beschreibung = spec
	log.Printf("Info: Read %d %s specifiaction records", len(spec), version)

	return set, nil
}
