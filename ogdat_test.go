package ogdat

import (
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
