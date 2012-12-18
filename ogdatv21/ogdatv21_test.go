package ogdatv21

import "testing"

func TestLoadISOFile(t *testing.T) {
	if spec := loadogdatv21spec(ogdatv21specfile); spec != nil {
		if spec != nil {
			if nspec := len(spec); nspec != 31 {
				t.Errorf("Should contain 31 records but found %d", nspec)
			}
		} else {
			t.Log("OGDATV21 specification file not found, so reading not tested")
		}

	}
}
