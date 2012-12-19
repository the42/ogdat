package ogdatv21

import "testing"

func TestLoadOGDATV21SpecFile(t *testing.T) {
	if spec := loadogdatv21spec(ogdatv21specfile); spec != nil {
		if nspec := len(spec); nspec != 31 {
			t.Errorf("Should contain 31 records but found %d", nspec)
		} else {
			// t.Error("SUCCESS")
		}
	} else {
		t.Log("Specification file %s not loaded", ogdatv21specfile)
	}
}
