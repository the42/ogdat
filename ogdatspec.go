package ogdat

const (
	Version10 = "OGD Austria Metadata 1.0" // Version 1.0: 24.10.2011
	Version11 = "OGD Austria Metadata 1.1" // Version 1.1: 12.03.2012
	Version20 = "OGD Austria Metadata 2.0" // Version 2.0: 10.10.2012
	Version21 = "OGD Austria Metadata 2.1" // Version 2.1: 15.10.2012
)

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
	Anzahl        int
	Defintion_DE  string
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
