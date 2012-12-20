package ogdat

import "testing"

const ogdatv21specfile = "ogdat_spec-2.1.csv"

func TestLoadOGDATSpecFile(t *testing.T) {
	if spec := loadogdatspec("v21", ogdatv21specfile); spec != nil {
		if nspec := len(spec); nspec != 31 {
			t.Errorf("Should contain 31 records but found %d", nspec)
		} else {
			// t.Error("SUCCESS")
		}
	} else {
		t.Logf("Specification file %s not loaded", ogdatv21specfile)
		t.Fail()
	}
}
