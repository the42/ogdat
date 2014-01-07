package ogdatv22

import (
	"github.com/the42/ogdat"
	"reflect"
)

const Version = "OGD Austria Metadata 2.2" // Version 2.2: 15.10.2012
const specfile = "ogdat_spec-2.2.csv"

type Extras struct {
	// Core
	Metadata_Identifier *ogdat.Identifier        `json:"metadata_identifier" ogdat:"ID:1"` // CKAN uses since API Version 2 a UUID V4, cf. https://github.com/okfn/ckan/blob/master/ckan/model/types.py
	Metadata_Modified   *ogdat.Time              `json:"metadata_modified" ogdat:"ID:5"`
	Categorization      *ogdat.MetaDataKategorie `json:"categorization" ogdat:"ID:10"`
	Begin_DateTime      *ogdat.Time              `json:"begin_datetime" ogdat:"ID:24"`

	// Optional
	Schema_Name           *string                `json:"schema_name" ogdat:"ID:2"`
	Schema_Language       *string                `json:"schema_language" ogdat:"ID:3"`     // always "ger"
	Schema_Characterset   *string                `json:"schema_characterset" ogdat:"ID:4"` // always "utf8", cf. https://www.ghrsst.org/files/download.php?m=documents&f=ISO%2019115%20.pdf
	Metadata_Linkage      *ogdat.MetaDataLinkage `json:"metadata_linkage" ogdat:"ID:6"`
	Attribute_Description *string                `json:"attribute_description" ogdat:"ID:12"`
	Maintainer_Link       *ogdat.Url             `json:"maintainer_link" ogdat:"ID:13"`
	Publisher             *string                `json:"publisher" ogdat:"ID:20"`
	Geographich_Toponym   *string                `json:"geographic_toponym" ogdat:"ID:22"`

	/*  ON/EN/ISO 19115:2003: westBL (344) & eastBL (345) & southBL (346) & northBL (347)
	 * Specifiaction says a WKT of POLYGON should be used, which would make a
	 * POLYGON ((-180.00 -90.00, 180.00 90.00)) but Example states
	 * POLYGON (-180.00 -90.00, 180.00 90.00)
	 * The situation is currently erroneous but unambigous, so we support both formats
	 */
	Geographic_BBox  *string      `json:"geographic_bbox" ogdat:"ID:23"`
	End_DateTime     *ogdat.Time  `json:"end_datetime" ogdat:"ID:25"`
	Update_Frequency *ogdat.Cycle `json:"update_frequency" ogdat:"ID:26"`
	Lineage_Quality  *string      `json:"lineage_quality" ogdat:"ID:27"`
	EnTitleDesc      *string      `json:"en_title_and_desc" ogdat:"ID:28"`
	License_Citation *string      `json:"license_citation" ogdat:"ID:30"`
}

type Resource struct {
	// Core
	Url    *ogdat.Url               `json:"url" ogdat:"ID:14"`
	Format *ogdat.ResourceSpecifier `json:"format" ogdat:"ID:15"`

	// Optional
	Name         *string     `json:"name" ogdat:"ID:16"`
	Created      *ogdat.Time `json:"created" ogdat:"ID:17"`
	LastModified *ogdat.Time `json:"last_modified" ogdat:"ID:18"`

	/*
	 * dcat:bytes a rdf:Property, owl:DatatypeProperty;
	 * rdfs:isDefinedBy <http://www.w3.org/ns/dcat>;
	 * rdfs:label "size in bytes";
	 * rdfs:comment "describe size of resource in bytes";
	 * rdfs:domain dcat:Distribution;
	 * rdfs:range xsd:integer .
	 */
	Size     *string `json:"size" ogdat:"ID:29"`
	Language *string `json:"language" ogdat:"ID:31"`
	/* Here we have a problem in spec 2.1. which says "nach ISO\IEC 10646-1", which means utf-8, utf-16 and utf-32.
	 * We would certainly support more encodings, as eg.
	 * ISO 19115 / B.5.10 MD_CharacterSetCode<> or
	 * http://www.iana.org/assignments/character-sets/character-sets.xml
	 */
	Encoding *string `json:"characterset" ogdat:"ID:32"`
}

type MetaData struct {
	// Core
	Title       *string      `json:"title" ogdat:"ID:8"`
	Description *string      `json:"notes" ogdat:"ID:9"`
	Schlagworte []ogdat.Tags `json:"tags" ogdat:"ID:11"`
	Maintainer  *string      `json:"maintainer" ogdat:"ID:19"`
	License     *string      `json:"license" ogdat:"ID:21"` // Sollte URI des Lizenzdokuments sein

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
	ogdat.RegisterFromCSVFile(Version, specfile)
}
