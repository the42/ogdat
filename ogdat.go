package ogdat

import (
	"code.google.com/p/go-uuid/uuid"
	"encoding/json"
	"net/url"

//	"time"
//	"fmt"
)

type Beschreibung struct {
	ID            int
	Bezeichner    string
	OGD_Kurzname  string
	CKAN_Feld     string
	Defintion_DE  string
	Erlauterung   string
	Beispiel      string
	ONA2270       string
	ISO19115      string
	RDFProperty   string
	Definition_EN string
}

type Kategorie struct {
	NumID       int
	ID          string
	PrettyName  string
	RDFProperty string
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

var categories = []Kategorie{Arbeit,
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

var categorymap map[string]*Kategorie

type Tags string
type ResourceSpecifier string

type Url struct {
	*url.URL
	Raw string
}

type Identfier struct {
	uuid.UUID
	Raw string
}

func (id *Identfier) String() string {
	return id.Raw
}

func (u *Url) UnmarshalJSON(data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	u.Raw = raw
	url, err := url.Parse(raw)
	if err != nil {
		return err
	}
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
		id.UUID = uuid
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
		return nil
	}
	*kat = *corecat
	return nil
}

type Extras struct {
	Metadata_Identifier Identfier   `json:"metadata_identifier"` // CKAN uses since API Version 2 a UUID V4, cf. https://github.com/okfn/ckan/blob/master/ckan/model/types.py
	Metadata_Modified   string      `json:"metadata_modified"`
	Categorization      []Kategorie `json:"categorization"`
	Begin_DateTime      string      `json:"begin_datetime"`
}

type Resource struct {
	URL    *Url              `json:"url"`
	Format ResourceSpecifier `json:"format"`
}

type Core struct {
	Title       string `json:"title"`
	Description string `json:"notes"`

	Schlagworte []Tags `json:"tags"`

	Maintainer string `json:"maintainer"`
	License    string `json:"license"` // Sollte URI des Lizenzdokuments sein
	Extras     `json:"extras"`
	Resource   []Resource `json:"resources"`
}

func init() {
	categorymap = make(map[string]*Kategorie)

	for _, val := range categories {
		categorymap[val.ID] = &val
	}
}
