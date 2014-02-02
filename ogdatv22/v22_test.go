package ogdatv22

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
	{ // Test, when all fields (required & optional ones) are empty
		&checkRequest{"allempty.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Error, OGDID: -1}, {Type: ogdat.Error, OGDID: 1}, {Type: ogdat.Error, OGDID: 5}, {Type: ogdat.Error, OGDID: 8}, {Type: ogdat.Error, OGDID: 9}, {Type: ogdat.Warning, OGDID: 10}, {Type: ogdat.Warning, OGDID: 11}, {Type: ogdat.Error, OGDID: 19}, {Type: ogdat.Error, OGDID: 21}, {Type: ogdat.Error, OGDID: 24}}},
	},
	{ // POLYGON may be specified with two (like the spec) or with one enclosing pair of brackets. Here test if one is ok
		&checkRequest{"file23a.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Info | ogdat.FetchableUrl, OGDID: 6},
			{Type: ogdat.Info | ogdat.FetchableUrl, OGDID: 13}}},
	},
	{ // POLYGON may only be specified in all caps
		&checkRequest{"file23b.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Info | ogdat.FetchableUrl, OGDID: 6},
			{Type: ogdat.Info | ogdat.FetchableUrl, OGDID: 13}, {Type: ogdat.Error, OGDID: 23}}},
	},
	{ // Begin and end point of polygon must match (closed polygon)
		&checkRequest{"file23c.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Info | ogdat.FetchableUrl, OGDID: 6},
			{Type: ogdat.Info | ogdat.FetchableUrl, OGDID: 13}, {Type: ogdat.Error, OGDID: 23}}},
	},
	{ // metadata_original_portal must start with urn://data., this one does not
		&checkRequest{"file33a.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Info | ogdat.FetchableUrl, OGDID: 6},
			{Type: ogdat.Info | ogdat.FetchableUrl, OGDID: 13},
			{Type: ogdat.Info | ogdat.FetchableUrl, OGDID: 33}, {Type: ogdat.Warning, OGDID: 33}}},
	},
	{ // metadata_original_portal must start with urn://data., this one does
		&checkRequest{"file33b.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Info | ogdat.FetchableUrl, OGDID: 6},
			{Type: ogdat.Info | ogdat.FetchableUrl, OGDID: 13},
			{Type: ogdat.Info | ogdat.FetchableUrl, OGDID: 33}}},
	},
	{ // maintainer_email must be a valid email address, this one is not
		&checkRequest{"file34a.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Info | ogdat.FetchableUrl, OGDID: 6},
			{Type: ogdat.Info | ogdat.FetchableUrl, OGDID: 13},
			{Type: ogdat.Warning, OGDID: 34}}},
	},
	{ // maintainer_email must be a valid email address, this one is
		&checkRequest{"file34b.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Info | ogdat.FetchableUrl, OGDID: 6},
			{Type: ogdat.Info | ogdat.FetchableUrl, OGDID: 13}}},
	},
	{ // This test is to check a metadata file in which every entry is OK
		&checkRequest{"fullandok.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Info | ogdat.FetchableUrl, OGDID: 6},
			{Type: ogdat.Info | ogdat.FetchableUrl, OGDID: 13}}},
	},
}

func TestCheck(t *testing.T) {

	for numtest, val := range checkTests {
		if val.out != nil {
			file, err := os.Open(path.Join("./testfiles", val.in.filename))
			if err != nil {
				t.Fatal(err)
			}
			ogdjsonmd, err := ioutil.ReadAll(file)
			if err != nil {
				t.Fatal(err)
			}
			md := &MetaData{}
			if err := json.Unmarshal(ogdjsonmd, md); err != nil {
				t.Fatalf("%s: Can't unmarshall byte stream: %s\n", val.in.filename, err)
			}
			msgs, err := md.Check(val.in.followlinks)

			testlen := len(val.out.message)
			retlen := len(msgs)
			if testlen != retlen {
				t.Fatalf("TestCheck [%d] (%s): ungleiche Anzahl an Fehlermeldungen. Test:%d, Return:%d", numtest, val.in.filename, testlen, retlen)
			}

			for i := 0; i < testlen; i++ {
				testtype := val.out.message[i].Type
				testid := val.out.message[i].OGDID
				rettype := msgs[i].Type
				retid := msgs[i].OGDID
				if testtype != rettype || testid != retid {
					t.Fatalf("TestCheck [%d] (%s): [Test.Type=%d, Test.OGDID=%d | Return.Type=%d, Return.OGDID=%d]", numtest, val.in.filename, testtype, testid, rettype, retid)
				}
			}
			file.Close()
		}
	}
}
