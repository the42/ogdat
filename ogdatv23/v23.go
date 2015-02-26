package ogdatv23

import (
	"github.com/the42/ogdat"
	"reflect"
)

const Version = "OGD Austria Metadata 2.3" // Version 2.3: 6.11.2014
const specfile = "ogdat_spec-2.3.csv"

type Extras struct {
	// Core
	Metadata_Identifier *ogdat.Identifier        `json:"metadata_identifier" ogdat:"ID:1"` // CKAN uses since API Version 2 a UUID V4, cf. https://github.com/okfn/ckan/blob/master/ckan/model/types.py
	Metadata_Modified   *ogdat.Time              `json:"metadata_modified" ogdat:"ID:5"`
	Categorization      *ogdat.MetaDataKategorie `json:"categorization" ogdat:"ID:10"`
	Begin_DateTime      *ogdat.Time              `json:"begin_datetime" ogdat:"ID:24"`
	// Mandatory as of V2.3
	Publisher *string `json:"publisher" ogdat:"ID:20"`

	// Optional
	Schema_Name           *string                `json:"schema_name" ogdat:"ID:2"`
	Schema_Language       *string                `json:"schema_language" ogdat:"ID:3"`     // always "ger"
	Schema_Characterset   *string                `json:"schema_characterset" ogdat:"ID:4"` // always "utf8", cf. https://www.ghrsst.org/files/download.php?m=documents&f=ISO%2019115%20.pdf
	Metadata_Linkage      *ogdat.MetaDataLinkage `json:"metadata_linkage" ogdat:"ID:6"`
	Attribute_Description *string                `json:"attribute_description" ogdat:"ID:12"`
	Maintainer_Link       *ogdat.Url             `json:"maintainer_link" ogdat:"ID:13"`
	Geographich_Toponym   *string                `json:"geographic_toponym" ogdat:"ID:22"`
	Geographic_BBox       *string                `json:"geographic_bbox" ogdat:"ID:23"`
	End_DateTime          *ogdat.Time            `json:"end_datetime" ogdat:"ID:25"`
	Update_Frequency      *ogdat.Cycle           `json:"update_frequency" ogdat:"ID:26"`
	Lineage_Quality       *string                `json:"lineage_quality" ogdat:"ID:27"`
	EnTitleDesc           *string                `json:"en_title_and_desc" ogdat:"ID:28"`
	License_Citation      *string                `json:"license_citation" ogdat:"ID:30"`

	// new as of V2.2
	Metadata_OriginalPortal *ogdat.Url `json:"metadata_original_portal" ogdat:"ID:33"`
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
	Encoding *string `json:"characterset" ogdat:"ID:32"`
}

type MetaData struct {
	// Core
	Title       *string      `json:"title" ogdat:"ID:8"`
	Description *string      `json:"notes" ogdat:"ID:9"`
	Schlagworte []ogdat.Tags `json:"tags" ogdat:"ID:11"`
	Maintainer  *string      `json:"maintainer" ogdat:"ID:19"`
	License     *string      `json:"license" ogdat:"ID:21"` // Sollte URI des Lizenzdokuments sein

	// Optional, new as of V2.2
	Maintainer_Email *ogdat.Url `json:"maintainer_email" ogdat:"ID:34"`

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

func (md *MetaData) MinimalMetadata() *ogdat.MinimalMetaData {

	minimd := &ogdat.MinimalMetaData{Description: md.Description,
		Extras: ogdat.Extras{Schema_Name: md.Schema_Name,
			Publisher:           md.Publisher,
			Geographic_BBox:     md.Geographic_BBox,
			Geographich_Toponym: md.Geographich_Toponym}}

	if md.Metadata_Identifier != nil {
		s := md.Metadata_Identifier.String()
		minimd.Metadata_Identifier = &s
	}
	if md.Maintainer_Link != nil {
		s := md.Maintainer_Link.String()
		minimd.Maintainer_Link = &s
	}

	minimd.Categorization = md.Categorization
	return minimd
}

func init() {
	ogdat.RegisterFromCSVFile(Version, specfile)
}
