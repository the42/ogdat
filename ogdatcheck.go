package ogdat

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	Info    = 1 << iota
	Warning = 1 << iota
	Error   = 1 << iota
)

const (
	StructuralError  = 0x8000
	NoDataatUrlError = 0x4000
	FetchableUrl     = 0x2000
	FetchSuccess     = 0x1000
)

var isolangfilemap map[string]*ISO6392Lang = nil

type ISO6392Lang struct {
	Code, Identifier string
}

func CheckISOLanguage(lang string) bool {
	const iso639file = "ISO-639-2_utf-8.txt"
	if isolangfilemap == nil {
		var err error
		if isolangfilemap, err = loadisolanguagefile(iso639file); err != nil {
			panic(fmt.Sprintf("Can not load ISO language file '%s'", iso639file))
		}
	}
	_, ok := isolangfilemap[lang]
	return ok
}

func loadisolanguagefile(filename string) (isolangfilemap map[string]*ISO6392Lang, _ error) {

	reader, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer reader.Close()
	isolangfilemap = make(map[string]*ISO6392Lang)
	csvreader := csv.NewReader(reader)
	csvreader.Comma = '|'

	for record, err := csvreader.Read(); err != io.EOF; record, err = csvreader.Read() {
		isorecord := &ISO6392Lang{Code: record[0], Identifier: record[3]}
		isolangfilemap[isorecord.Code] = isorecord
		if len(record[1]) > 0 {
			isorecord = &ISO6392Lang{Code: record[1], Identifier: record[3]}
			isolangfilemap[record[1]] = isorecord
		}
	}
	log.Printf("Info: Read %d ISO language records", len(isolangfilemap))

	return
}

var ianaencmap map[string]struct{} = nil

// CheckIANAEncoding will try to match and input of enc against the specified encodings found at http://www.iana.org/assignments/character-sets/character-sets.xml
// The file at http://www.iana.org/assignments/character-sets/character-sets.xml is retrieved by a shell scripte,
// converted to all-lower case and sorted for unique entries. Thus the encoding enc against which will be checked,
// is converted to lower case and then compared to the IANA-encodings
func CheckIANAEncoding(enc string) bool {
	const ianaencfile = "character-sets.txt"
	if ianaencmap == nil {
		var err error
		if ianaencmap, err = loadianaencodingfile(ianaencfile); err != nil {
			panic(fmt.Sprintf("Can not load IANA encoding definition file '%s': %s", ianaencfile, err))
		}
	}
	enc = strings.ToLower(enc)
	_, ok := ianaencmap[enc]
	if !ok {
		enc = strings.Replace(enc, "-", "", -1)
		_, ok = ianaencmap[enc]
	}
	return ok
}

func loadianaencodingfile(filename string) (ianamap map[string]struct{}, _ error) {
	reader, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	ianamap = make(map[string]struct{})
	bufreader := bufio.NewReader(reader)
	delim := byte('\n')

	for line, err := bufreader.ReadString(delim); err != io.EOF; line, err = bufreader.ReadString(delim) {
		// ReadString includes the delimeter, get rid of it
		line = line[:len(line)-1]
		// normalize by lower casing
		line = strings.ToLower(line)

		ianamap[line] = struct{}{}
	}
	log.Printf("Info: Read %d IANA encoding names", len(ianamap))
	return
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type CheckInfo struct {
	Status, Position int
	Context          string
}

func (c *CheckInfo) Error() string {
	return c.Context
}

func strrange(minrange, maxrange, idx int, s string) string {
	if minrange > maxrange {
		panic("minrange > maxrange")
	}

	if idx > len(s) {
		idx = len(s)
	}

	var prepend string
	start := idx + minrange
	if start < 0 {
		start = 0
	} else {
		prepend = "..."
	}

	var postpone string
	end := idx + maxrange
	if end > len(s) {
		end = len(s)
	} else {
		postpone = "..."
	}
	return prepend + s[start:end] + postpone
}

var regexphtmlcodecheck = regexp.MustCompile(`</{0,1}\w+.*('|"|)>`)
var regexphtmlescape = regexp.MustCompile(`&\w{1,10};|&#\d{1,6};`)
var regexpurlencode = regexp.MustCompile(`%[0-9a-fA-F][0-9a-fA-F]`)
var regexpposixescape = regexp.MustCompile(`\\n|\\b|\\v|\\t|\\r`)

// return values are:
// ok = false indicates sthg. was wrong in which case error will not be nil
//
// error: if is of type CheckError:
// Status: 1 = Info, 2 = Warning, 3 = Error
// Position: beginning position of offending input
// message: An error message describing the problem
func CheckOGDTextStringForSaneCharacters(str string) (ok bool, _ error) {
	if !utf8.ValidString(str) {
		return false, &CheckInfo{Error, 0, "Zeichenfolge ist nicht durchgängig gültig als UTF8 kodiert"}
	}
	for idx, val := range str {
		if val == unicode.ReplacementChar {
			return false, &CheckInfo{Error, idx, fmt.Sprintf("Ungültige Unicode-Sequenz: '0x%x' (Bereich '%s')", val, strrange(-20, 20, idx, str))}
		}
	}

	if idx := regexphtmlcodecheck.FindStringIndex(str); idx != nil {
		return false, &CheckInfo{Warning, idx[0], fmt.Sprintf("Mögliche HTML-Sequenz: '%s'", strrange(-10, 10, idx[0], str))}
	}
	if idx := regexphtmlescape.FindStringIndex(str); idx != nil {
		return false, &CheckInfo{Warning, idx[0], fmt.Sprintf("Mögliche HTML-Escapes: '%s'", strrange(-8, 8, idx[0], str))}
	}
	if idx := regexpurlencode.FindStringIndex(str); idx != nil {
		return false, &CheckInfo{Warning, idx[0], fmt.Sprintf("Mögliche Url-Escapes: '%s'", strrange(-6, 6, idx[0], str))}
	}
	if idx := regexpposixescape.FindStringIndex(str); idx != nil {
		return false, &CheckInfo{Warning, idx[0], fmt.Sprintf("Mögliche Posix-Escapes: '%s'", strrange(-6, 6, idx[0], str))}
	}
	return true, nil
}

var regexpbboxWKT = regexp.MustCompile(`^POLYGON\s{0,1}\({1,2}\s{0,2}[-+]?[0-9]*\.?[0-9]+\s{1,2}[-+]?[0-9]*\.?[0-9]+,\s{0,2}[-+]?[0-9]*\.?[0-9]+\s{1,2}[-+]?[0-9]*\.?[0-9]+\s{0,2}\){1,2}$`)

func CheckOGDBBox(str string) (bool, error) {
	if !utf8.ValidString(str) {
		return false, &CheckInfo{Error, -1, "Zeichenfolge ist nicht durchgängig gültig als UTF8 kodiert"}
	}
	if idx := regexpbboxWKT.FindStringIndex(str); idx == nil {
		return false, &CheckInfo{Error, -1, fmt.Sprintf("Keine gültige WKT-Angabe einer BoundingBox: '%s'", str)}
	}
	return true, nil
}

var regexpEMail = regexp.MustCompile(`(?i)^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}$`)

func FetchHead(url string) (bool, CheckInfo) {

	var info CheckInfo
	var err error
	var resp *http.Response

	for i := 0; i < 2; i++ {
		if i == 0 {
			resp, err = http.Head(url)
		} else {
			resp, err = http.Get(url)
		}
		if err != nil || resp.StatusCode != 200 {
			continue
		} else {
			break
		}
	}

	if err != nil {
		info = CheckInfo{Status: Error | FetchableUrl | NoDataatUrlError, Position: -1, Context: fmt.Sprintf("'%s': URL kann nicht aufgelöst werden: '%s'", url, err)}
	} else if sc := resp.StatusCode; sc != 200 {
		info = CheckInfo{Status: Error | FetchableUrl | NoDataatUrlError, Position: -1, Context: fmt.Sprintf("'%s': HEAD request liefert nicht-OK Statuscode '%d'", url, sc)}
	} else {
		info = CheckInfo{Status: Info | FetchableUrl | FetchSuccess, Position: -1, Context: url}
	}

	return (info.Status & (Info | FetchSuccess)) == (Info | FetchSuccess), info
}

func CheckUrl(url string, followhttplink bool) (bool, []CheckInfo) {
	// it's a contact point if it's a http-link (starts with "http(s)" )
	var checkmessages []CheckInfo
	if len(url) >= 4 && url[:4] == "http" {
		urlinfo := CheckInfo{Info | FetchableUrl, -1, url}
		checkmessages = append(checkmessages, urlinfo)

		ok := true
		if followhttplink {
			ok, urlinfo = FetchHead(urlinfo.Context)
			checkmessages = append(checkmessages, urlinfo)
		}
		return ok, checkmessages
	}
	// it's a contact point if it's an email address
	if idx := regexpEMail.FindStringIndex(url); idx != nil {
		return true, nil
	}

	if len(url) == 0 {
		checkmessages = append(checkmessages, CheckInfo{Status: Error, Position: -1, Context: "kein Wert für Link angegeben (Länge 0)"})
		return false, checkmessages
	}

	checkmessages = append(checkmessages, CheckInfo{Status: Warning, Position: -1, Context: fmt.Sprintf("vermutlich keine gültige Web- oder E-Mail Adresse: '%s' (Auszug)", url[:min(20, len(url))])})

	return false, checkmessages
}

type CheckMessage struct {
	Type    int // 1 = Info, 2 = Warning, 4 = Error, ...
	Text    string
	OGDID   int
	Context string
}

type Checker interface {
	Check(bool) ([]CheckMessage, error)
}

func AppendcheckerrorTocheckmessage(msgs []CheckMessage, checkresults []CheckInfo, ID int, prepend string) []CheckMessage {
	for _, result := range checkresults {
		msgs = append(msgs, CheckMessage{
			Type:  result.Status,
			OGDID: ID,
			Text:  prepend + result.Context})
	}
	return msgs
}

func Loadogdatspec(version, filename string) (*OGDSet, error) {
	reader, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	csvreader := csv.NewReader(reader)
	csvreader.Comma = '|'
	csvreader.LazyQuotes = true

	// Read the first line and use it as the labels for the items to load
	record, err := csvreader.Read()
	if err != nil {
		return nil, err
	}

	set := &OGDSet{Label: record}

	spec := make([]*Beschreibung, 0)
	for record, err = csvreader.Read(); err != io.EOF; record, err = csvreader.Read() {
		id, _ := strconv.Atoi(record[0])
		var occ Occurrence
		switch record[12][0] {
		case 'R':
			occ = OccRequired
		case 'O':
			occ = OccOptional
		}
		descrecord := NewBeschreibung(id, occ, version)

		descrecord.Bezeichner = record[1]
		descrecord.OGD_Kurzname = record[2]
		descrecord.CKAN_Feld = record[3]
		descrecord.Anzahl = record[4]
		descrecord.Definition_DE = record[5]
		descrecord.Erlauterung = record[6]
		descrecord.Beispiel = record[7]
		descrecord.ONA2270 = record[8]
		descrecord.ISO19115 = record[9]
		descrecord.RDFProperty = record[10]
		descrecord.Definition_EN = record[11]

		spec = append(spec, descrecord)
	}
	set.Beschreibung = spec
	log.Printf("Info: Read %d %s specification records", len(spec), version)

	return set, nil
}
