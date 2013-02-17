package ogdatv21

import (
	"encoding/json"
	// "fmt"
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
	{
		&checkRequest{"allempty.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: 4, OGDID: -1}, {Type: 3, OGDID: 1}, {Type: 3, OGDID: 5}, {Type: 3, OGDID: 8}, {Type: 3, OGDID: 9}, {Type: 2, OGDID: 10}, {Type: 2, OGDID: 11}, {Type: 3, OGDID: 19}, {Type: 3, OGDID: 21}, {Type: 3, OGDID: 24}}},
	},
	{
		&checkRequest{"fullandok.json", false},
		&checkResponse{message: []ogdat.CheckMessage{}},
	},
}

func TestCheck(t *testing.T) {

	for numtest, val := range checkTests {
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
			t.Fatalf("Can't unmarshall byte stream: %s\n", err)
		}
		msgs, err := md.Check(val.in.followlinks)
		if val.out != nil {
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
		}
		file.Close()
	}
}
