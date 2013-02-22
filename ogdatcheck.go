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
	Info            = 1
	Warning         = 2
	Error           = 3
	StructuralError = 4
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

func CheckIANAEncoding(enc string) bool {
	const ianaencfile = "character-sets.txt"
	if ianaencmap == nil {
		var err error
		if ianaencmap, err = loadianaencodingfile(ianaencfile); err != nil {
			panic(fmt.Sprintf("Can not load IANA encoding definition file '%s'", ianaencfile))
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

type CheckError struct {
	Status, Position int
	message          string
}

func (ce *CheckError) Error() string {
	return ce.message
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

var regexphtmlcodecheck = regexp.MustCompile(`<\w+.*('|"|)>`)
var regexphtmlescape = regexp.MustCompile(`&\w{1,10};|&#\d{1,6};`)
var regexpurlencode = regexp.MustCompile(`%[0-9a-fA-F][0-9a-fA-F]`)
var regexpposixescape = regexp.MustCompile(`\\n|\\b|\\v|\\t`)

// return values are:
// ok = false indicates sthg. was wrong in which case error will not be nil
//
// error: if is of type CheckError:
// Status: 1 = Info, 2 = Warning, 3 = Error
// Position: beginning position of offending input
// message: An error message describing the problem
func CheckOGDTextStringForSaneCharacters(str string) (ok bool, _ error) {
	if !utf8.ValidString(str) {
		return false, &CheckError{Error, 0, "Zeichenfolge ist nicht durchgängig gültig als UTF8 kodiert"}
	}
	for idx, val := range str {
		if val == unicode.ReplacementChar {
			return false, &CheckError{Error, idx, fmt.Sprintf("Ungültige Unicode-Sequenz: '0x%x' (Bereich '%s')", val, strrange(-20, 20, idx, str))}
		}
	}

	if idx := regexphtmlcodecheck.FindStringIndex(str); idx != nil {
		return false, &CheckError{Warning, idx[0], fmt.Sprintf("Mögliche HTML-Sequenz: '%s'", str[idx[0]:min(20, idx[1]-idx[0])])}
	}
	if idx := regexphtmlescape.FindStringIndex(str); idx != nil {
		return false, &CheckError{Warning, idx[0], fmt.Sprintf("Mögliche HTML-Escapes: '%s'", str[idx[0]:min(15, idx[1]-idx[0])])}
	}
	if idx := regexpurlencode.FindStringIndex(str); idx != nil {
		return false, &CheckError{Warning, idx[0], fmt.Sprintf("Mögliche Url-Escapes: '%s'", str[idx[0]:min(8, idx[1]-idx[0])])}
	}
	if idx := regexpposixescape.FindStringIndex(str); idx != nil {
		return false, &CheckError{Warning, idx[0], fmt.Sprintf("Mögliche Posix-Escapes: '%s'", str[idx[0]:min(5, idx[1]-idx[0])])}
	}
	return true, nil
}

var regexpbboxWKT = regexp.MustCompile(`^POLYGON\s{0,1}\({1,2}\s{0,2}[-+]?[0-9]*\.?[0-9]+\s{1,2}[-+]?[0-9]*\.?[0-9]+,\s{0,2}[-+]?[0-9]*\.?[0-9]+\s{1,2}[-+]?[0-9]*\.?[0-9]+\s{0,2}\){1,2}$`)

func CheckOGDBBox(str string) (bool, error) {
	if !utf8.ValidString(str) {
		return false, &CheckError{Error, -1, "Zeichenfolge ist nicht durchgängig gültig als UTF8 kodiert"}
	}
	if idx := regexpbboxWKT.FindStringIndex(str); idx == nil {
		return false, &CheckError{Error, -1, fmt.Sprintf("Keine gültige WKT-Angabe einer BoundingBox: '%s'", str)}
	}
	return true, nil
}

// TODO: add switch for case-insenstive check
var regexpEMail = regexp.MustCompile(`^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}$`)

func CheckUrlContact(url string, followhttplink bool) (bool, error) {
	// it's a contact point if it's a http-link (starts with "http(s)" )
	if len(url) >= 4 && url[:4] == "http" {
		if followhttplink {
			resp, err := http.Head(url)
			if err != nil {
				return false, &CheckError{Error, -1, fmt.Sprintf("URL kann nicht aufgelöst werden: '%s'", err)}
			}
			if sc := resp.StatusCode; sc != 200 {
				return false, &CheckError{Error, -1, fmt.Sprintf("HEAD request liefert nicht-OK Statuscode '%d'", sc)}
			}
		}
		return true, nil
	}
	// it's a contact point if it's an email address
	if idx := regexpEMail.FindStringIndex(url); idx != nil {
		return true, nil
	}
	return false, &CheckError{Warning, -1, fmt.Sprintf("vermutlich keine gültige Web- oder E-Mail Adresse: '%s' (Auszug)", url[:min(20, len(url))])}
}

type CheckMessage struct {
	Type    int // 1 = Info, 2 = Warning, 3 = Error, 4 = StructuralError
	Text    string
	OGDID   int
	Context string
}

type Checker interface {
	Check(bool) ([]CheckMessage, error)
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
	log.Printf("Info: Read %d %s specifiaction records", len(spec), version)

	return set, nil
}
