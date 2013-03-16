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
	{ // invalid date
		&checkRequest{"file5.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Error, OGDID: 5}}},
	},
	{ // invalid characters in title
		&checkRequest{"file8.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Warning, OGDID: 8}}},
	},
	{ // invalid characters in description
		&checkRequest{"file9.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Warning, OGDID: 9}}},
	},
	{ // no entries for 'Kategorie' is a warning
		&checkRequest{"file10a.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Warning, OGDID: 10}}},
	},
	{ // no entries for 'Kategorie' is a warning
		&checkRequest{"file10b.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Warning, OGDID: 10}}},
	},
	{ // unknown entry for 'Kategorie'
		&checkRequest{"file10c.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Error, OGDID: 10}}},
	},
	{ // no entries for 'Schlagworte' is a warning
		&checkRequest{"file11a.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Warning, OGDID: 11}}},
	},
	{ // no entries for 'Schlagworte' is a warning
		&checkRequest{"file11b.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Warning, OGDID: 11}}},
	},
	{ // invalid characters for 'maintainer' and 'license'
		&checkRequest{"file19_21.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Warning, OGDID: 19}, {Type: ogdat.Warning, OGDID: 21}}},
	},
	{ // invalid time format for 'begin_datetime' and end_datetime
		&checkRequest{"file24_25.json", false},
		&checkResponse{message: []ogdat.CheckMessage{
			{Type: ogdat.Error, OGDID: 24},
			{Type: ogdat.Error, OGDID: 25},
		}},
	},
	//
	// core and extras - optional fields
	//
	{ // invalid characters in schema_name and no reference to version 2.0 or 2.1 in name specifier
		&checkRequest{"file2.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Warning, OGDID: 2}, {Type: ogdat.Info, OGDID: 2}}},
	},
	{ // schema language is german, specified as (GeR) which is ok as we check case-insensitive
		&checkRequest{"file3a.json", false},
		&checkResponse{message: []ogdat.CheckMessage{}},
	},
	{ // schema language is "xYz" which is an error. Only german allowed
		&checkRequest{"file3b.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Error, OGDID: 3}}},
	},
	{ // schema characterset specified as "utf-8": The specification is picky in this respect, as it refers to
		// specification ON/EN/ISO 19115:2003 mdC(4), which only knows about "utf8". We accept anycase utf-8 and utf8
		&checkRequest{"file4a.json", false},
		&checkResponse{message: []ogdat.CheckMessage{}},
	},
	{ // schema characterset specified as "utf-8": The specification is picky in this respect, as it refers to
		// specification ON/EN/ISO 19115:2003 mdC(4), which only knows about "utf8". We accept anycase utf-8 and utf8
		// This check must fail, as the test file contains an encoding which is not utf-8
		&checkRequest{"file4b.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Error, OGDID: 4}}},
	},
	{ // an empty string is not a valid link
		&checkRequest{"file6a.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Info, OGDID: 6}, {Type: ogdat.Error, OGDID: 6}}},
	},
	{ // a null as metadata_linkage is ok (it's optional)
		&checkRequest{"file6b.json", false},
		&checkResponse{message: []ogdat.CheckMessage{}},
	},
	{ // an empty array as metadata_linkage is ok (it's optional)
		&checkRequest{"file6c.json", false},
		&checkResponse{message: []ogdat.CheckMessage{}},
	},
	{ // an empty string as an element in a metadata_linkage array is ivalid
		&checkRequest{"file6d.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Error, OGDID: 6}}},
	},
	{ // a single element as metadata_linkage is acutally erroneous as per spec, but accepted by practice. Report it as info
		&checkRequest{"file6e.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Info, OGDID: 6}}},
	},
	{ // the field description must no be to short and must not contain escape characters (eg. \n, <br>)
		&checkRequest{"file12a.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Warning, OGDID: 12}, {Type: ogdat.Warning, OGDID: 12}}},
	},
	{ // empty maintainer link is an error
		&checkRequest{"file13a.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Error, OGDID: 13}}},
	},
	{ // unknown protocoll of maintainer link is a warning
		&checkRequest{"file13b.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Warning, OGDID: 13}}},
	},
	{ //
		&checkRequest{"file20_22_27_28_30.json", false},
		&checkResponse{message: []ogdat.CheckMessage{
			{Type: ogdat.Warning, OGDID: 20},
			{Type: ogdat.Warning, OGDID: 22},
			{Type: ogdat.Warning, OGDID: 27},
			{Type: ogdat.Warning, OGDID: 28},
			{Type: ogdat.Warning, OGDID: 30},
		}},
	},
	{ // POLYGON may be specified with two (like the spec) or with one enclosing pair of brackets. Here test if one is ok
		&checkRequest{"file23a.json", false},
		&checkResponse{message: []ogdat.CheckMessage{}},
	},
	{ // POLYGON may only be specified in all caps
		&checkRequest{"file23b.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Error, OGDID: 23},}},
	},
	{ // . is the only valid not ,
		&checkRequest{"file23c.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Error, OGDID: 23},}},
	},
	{ // unknown update frequency specification
		&checkRequest{"file26a.json", false},
		&checkResponse{message: []ogdat.CheckMessage{{Type: ogdat.Warning, OGDID: 26},}},
	},
	{ // english specification is ok
		&checkRequest{"file26b.json", false},
		&checkResponse{message: []ogdat.CheckMessage{}},
	},
	{ // an code from code-table is also ok
		&checkRequest{"file26c.json", false},
		&checkResponse{message: []ogdat.CheckMessage{}},
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
