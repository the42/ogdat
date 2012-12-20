package ogdat

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"strconv"
)

/*	Version10 = "OGD Austria Metadata 1.0" // Version 1.0: 24.10.2011
	Version11 = "OGD Austria Metadata 1.1" // Version 1.1: 12.03.2012
	Version20 = "OGD Austria Metadata 2.0" // Version 2.0: 10.10.2012
	Version21 = "OGD Austria Metadata 2.1" // Version 2.1: 15.10.2012
*/
type OGDSet []*Beschreibung

var specification = make(map[string]OGDSet)

type Occurrence int

const (
	OccUndef Occurrence = iota
	OccRequired
	OccOptional
)

func (desc *Beschreibung) Version() string {
	return desc.version
}

func (desc *Beschreibung) Occurrence() Occurrence {
	return desc.occurrence
}

type Beschreibung struct {
	ID            int
	Bezeichner    string
	OGD_Kurzname  string
	CKAN_Feld     string
	Anzahl        byte
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

func (set OGDSet) GetSpecForID(id int) *Beschreibung {
	if set != nil {
		for idx, elm := range set {
			if elm.ID == id {
				return set[idx]
			}
		}
	}
	return nil
}

func Register(version, specfile string) {
	specmap, _ := Loadogdatspec(version, specfile)
	specification[version] = specmap
}

func Loadogdatspec(version, filename string) (OGDSet, error) {
	reader, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	spec := make(OGDSet, 0)
	csvreader := csv.NewReader(reader)
	csvreader.Comma = '|'
	csvreader.LazyQuotes = true

	// skip the first line as it contains the field description
	record, err := csvreader.Read()

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
		descrecord.Anzahl = byte(record[4][0])
		descrecord.Definition_DE = record[5]
		descrecord.Erlauterung = record[6]
		descrecord.Beispiel = record[7]
		descrecord.ONA2270 = record[8]
		descrecord.ISO19115 = record[9]
		descrecord.RDFProperty = record[10]
		descrecord.Definition_EN = record[11]

		spec = append(spec, descrecord)
	}
	log.Printf("Info: Read %d %s specifiaction records", len(spec), version)

	return spec, nil
}
