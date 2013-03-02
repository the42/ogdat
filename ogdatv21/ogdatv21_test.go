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
	{ // This test is to check a metadata file in which every entry is OK
		&checkRequest{"fullandok.json", false},
		&checkResponse{message: []ogdat.CheckMessage{}},
	},
	{ // Test, when all fields (required & optional ones) are empty
		&checkRequest{"allempty.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: 4, OGDID: -1}, {Type: 3, OGDID: 1}, {Type: 3, OGDID: 5}, {Type: 3, OGDID: 8}, {Type: 3, OGDID: 9}, {Type: 2, OGDID: 10}, {Type: 2, OGDID: 11}, {Type: 3, OGDID: 19}, {Type: 3, OGDID: 21}, {Type: 3, OGDID: 24}}},
	},
	//
	// resources
	//
	{ // invalid url
		&checkRequest{"file14a.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Error, OGDID: 14}}},
	},
	{ // unknown protocoll in url
		&checkRequest{"file14b.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Warning, OGDID: 14}}},
	},
	{ // invalid characters in resource format specifier
		&checkRequest{"file15a1.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Warning, OGDID: 15}, {Type: ogdat.Warning, OGDID: 15}}},
	},
	{ // CheckOGDTextStringForSaneCharacters: HTML-Escapes (&#319;)
		&checkRequest{"file16a.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Warning, OGDID: 16}}},
	},
	{ // CheckOGDTextStringForSaneCharacters: Posix-Escapes (\n)
		&checkRequest{"file16b.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Warning, OGDID: 16}}},
	},
	{ // CheckOGDTextStringForSaneCharacters: HTML-Sequenz (<p><br>)
		&checkRequest{"file16c.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Warning, OGDID: 16}}},
	},
	{ // CheckOGDTextStringForSaneCharacters: URL-Escape()
		&checkRequest{"file16d.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Warning, OGDID: 16}}},
	},
	{ // invalid date format
		&checkRequest{"file17_18.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Error, OGDID: 17}, {Type: ogdat.Error, OGDID: 18}}},
	},
	{ // invalid resource size specification (onyl digits allowed)
		&checkRequest{"file29.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Error, OGDID: 29}}},
	},
	{ // unknown iso639-2 language code
		&checkRequest{"file31.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Error, OGDID: 3}}},
	},
	{ // check that utf-8 and utf16 are valid resource encodings, big5 accepted as valid for IANA and 'klingon' invalid
		&checkRequest{"file32.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Warning, OGDID: 32}, {Type: ogdat.Error, OGDID: 32}}},
	},
	//
	// core and extras
	//
	{ // non-uuid metadata identifier
		&checkRequest{"file1.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Error, OGDID: 1}}},
	},
	{
		&checkRequest{"file1_test.json", false},
		nil,
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
				t.Fatalf("Can't unmarshall byte stream: %s\n", err)
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
