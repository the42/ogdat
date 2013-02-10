package ogdatv21

import (
	"encoding/json"
	"github.com/the42/ogdat"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

type checkRequest struct {
	filename    string
	followlinks bool
}

type checkResponse struct {
	message []ogdat.CheckMessage
	err     error
}

type checkTest struct {
	in  *checkRequest
	out *checkResponse
}

var checkTests = []checkTest{
	{
		&checkRequest{"file1_test.json", false},
		nil,
	},
}

func TestCheck(t *testing.T) {
	md := &MetaData{}

	for _, val := range checkTests {
		file, err := os.Open(path.Join("./testfiles", val.in.filename))
		if err != nil {
			t.Fatal(err)
		}
		ogdjsonmd, err := ioutil.ReadAll(file)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(ogdjsonmd, md); err != nil {
			t.Fatalf("Can't unmarshall byte stream: %s\n", err)
		}
		res, err := md.Check(val.in.followlinks)
		if val.out != nil {
			// TODO: compare results
			_ = res
		}
		file.Close()
	}
}
