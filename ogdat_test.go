package ogdat

import (
	"fmt"
	"os"
	"reflect"
	"testing"
)

const ogdatv21specfile = "ogdat_spec-2.1.csv"

func TestLoadOGDATSpecFile(t *testing.T) {
	if spec, _ := Loadogdatspec("v21", ogdatv21specfile); spec != nil {
		if nspec := len(spec.Beschreibung); nspec != 31 {
			t.Errorf("LoadOGDATSpecFile: Should contain 31 records but found %d", nspec)
		} else {
			// t.Error("SUCCESS")
		}
	} else {
		t.Logf("LoadOGDATSpecFile: Specification file %s not loaded", ogdatv21specfile)
		t.Fail()
	}
}

type checkIANAEncodingTest struct {
	in  string
	out bool
}

var checkIANAEncodingTests = []checkIANAEncodingTest{
	{"ISO-8859-1", true},
	{"latin1", true},
	{"abacab", false},
}

func TestIANACheck(t *testing.T) {
	for idx, test := range checkIANAEncodingTests {
		if result := CheckIANAEncoding(test.in); result != test.out {
			t.Errorf("CheckIANAEncoding-[%d]: Encoding '%s': expected '%v' but got '%v'", idx, test.in, test.out, result)
		}
	}
}

type My struct {
	A struct{} `json:"Hallo Welt" ogdat:"ID:1, yow dawg"`
	B struct{} `ogdat:"ID:2, yow dawg"`
	C struct{} `ogdat:"ID:3"`
}

func TestGetIDFromMetaDataStructField(t *testing.T) {
	x := &My{}
	if f, ok := reflect.TypeOf(x).Elem().FieldByName("A"); ok {
		if id := GetIDFromMetaDataStructField(f); id != 1 {
			t.Errorf("GetIDFromMetaDataStructField: Expected 1, got %d", id)
		}
	}
	if f, ok := reflect.TypeOf(x).Elem().FieldByName("B"); ok {
		if id := GetIDFromMetaDataStructField(f); id != 2 {
			t.Errorf("GetIDFromMetaDataStructField: Expected 2, got %d", id)
		}
	}
	if f, ok := reflect.TypeOf(x).Elem().FieldByName("C"); ok {
		if id := GetIDFromMetaDataStructField(f); id != 3 {
			t.Errorf("GetIDFromMetaDataStructField: Expected 3, got %d", id)
		}
	}

}

type minimalMetadataEncodingTest struct {
	jsonfilename string
	out          *MinimalMetaData
}

func pstr(s string) (p *string) {
	p = new(string)
	*p = s
	return
}

var checkminimalMetadataEncodingTest = []minimalMetadataEncodingTest{
	{"minimalmetadata.json", &MinimalMetaData{Description: pstr("Tabelle der Arbeitslosen in Linz-Stadt"),
		Extras: Extras{Metadata_Identifier: pstr("9470466e-2fbd-4191-be2a-40238b77fadf"),
			Schema_Name:         pstr("OGD Austria Metadata 2.1"),
			Maintainer_Link:     pstr("open.commons@linz.at"),
			Publisher:           pstr("Stadt Linz"),
			Geographic_BBox:     pstr("POLYGON"),
			Geographich_Toponym: pstr("Linz"),
			Categorization:      []string{"wirtschaft-und-tourismus"}}},
	},
}

func stringifyminimalmetadata(mmd *MinimalMetaData) (ms string) {
	if mmd.Description != nil {
		ms = fmt.Sprintf("Beschreibung: %s\n", *mmd.Description)
	}
	if mmd.Metadata_Identifier != nil {
		ms += fmt.Sprintf("Identifier: %s\n", *mmd.Metadata_Identifier)
	}
	if mmd.Schema_Name != nil {
		ms += fmt.Sprintf("Schema_Name: %s\n", *mmd.Schema_Name)
	}
	if mmd.Maintainer_Link != nil {
		ms += fmt.Sprintf("Maintainer_Link: %s\n", *mmd.Maintainer_Link)
	}
	if mmd.Publisher != nil {
		ms += fmt.Sprintf("Publisher: %s\n", *mmd.Publisher)
	}
	if mmd.Geographic_BBox != nil {
		ms += fmt.Sprintf("BBox: %s\n", *mmd.Geographic_BBox)
	}
	if mmd.Geographich_Toponym != nil {
		ms += fmt.Sprintf("Toponym: %s\n", *mmd.Geographich_Toponym)
	}
	ms += fmt.Sprintf("Categorization: %s\n", mmd.Categorization)
	return
}

func equalsminimalmetadata(a, b *MinimalMetaData) bool {

	as := stringifyminimalmetadata(a)
	bs := stringifyminimalmetadata(b)
	return as == bs
}

func TestMinimalMetaDataforJSONStream(t *testing.T) {
	for idx, test := range checkminimalMetadataEncodingTest {
		file, err := os.Open(test.jsonfilename)
		if err != nil {
			t.Errorf("TestMinimalMetaDataforJSONStream-[%d]: Cannot open file: :%s", idx, err)
			continue
		}
		result, err := MinimalMetaDataforJSONStream(file)
		if err != nil {
			t.Errorf("TestMinimalMetaDataforJSONStream-[%d]: %s", idx, err)
			continue
		}
		if !equalsminimalmetadata(test.out, result) {
			t.Errorf("TestMinimalMetaDataforJSONStream-[%d]: expected '%s', got '%s'", idx, stringifyminimalmetadata(test.out), stringifyminimalmetadata(result))
			continue
		}
	}
}

type ogdversionfromminimalmetadata struct {
	in  string
	out string
}

var checkogdversionfromminimalmetadata = []ogdversionfromminimalmetadata{
	{"Metadata 4", "4"},
	{"Metadata 4.4", "4.4"},
	{"Metadata 7 vom 8.3", ""},
	{"Blah", ""},
}

func TestOGDVersionfromString(t *testing.T) {
	for idx, test := range checkogdversionfromminimalmetadata {
		out := OGDVersionfromString(test.in)
		if out != test.out {

			t.Errorf("TestOGDVersionfromString-[%d]: Expected %s, got %s", idx, test.out, out)
		}
	}
}
