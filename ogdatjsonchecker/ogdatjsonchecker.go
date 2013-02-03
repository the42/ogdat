package main

import (
	"flag"
	"github.com/the42/ogdat/ogdatv21"
	"github.com/the42/ogdat"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
)

var mdsource = flag.String("if", "", "Einzelne, CKAN-compatible, JSON-Beschreibung eines Metadatensatzes. Kann eine lokale Datei sein, oder über http/https bezogen werden. Standard: stdin")
var version = flag.String("version", ogdatv21.Version, "Version, nach der das OGD Metadatendokument überprüft werden soll")

func main() {
	flag.Parse()
	var reader io.Reader

	// 1. if no source is given or source is empty, use stdin
	if *mdsource == "" {
		reader = os.Stdin
	} else {
		// 2. if the data is available as http or https
		if strings.Index(strings.TrimSpace(*mdsource), "http") == 0 {
			resp, err := http.Get(*mdsource)
			if err != nil {
				log.Printf("Can't fetch from '%s': %s\n", *mdsource, err)
				os.Exit(1)
			}
			defer resp.Body.Close()
			reader = resp.Body
		} else {
			// 3. else try to open it as a file
			file, err := os.Open(*mdsource)
			if err != nil {
				log.Printf("Can't open '%s': %s\n", *mdsource, err)
				os.Exit(1)
			}
			defer file.Close()
			reader = file
		}
	}

	ogdjsonmd, err := ioutil.ReadAll(reader)
	if err != nil {
		log.Printf("Can't read from stream: %s\n", err)
		os.Exit(1)
	}

	// TODO: according to which Version will the data be checked?
	set := ogdat.GetOGDSetForVersion(ogdatv21.Version)
	md := &ogdatv21.MetaData{}
	
	if err := json.Unmarshal(ogdjsonmd, md); err != nil {
		log.Printf("Can't unmarshall byte stream: %s\n", err)
		os.Exit(1)
	}
	// TODO: follow links should be a command line switch
	msgs, err := md.Check(false)
	if err != nil {
		log.Printf("Unexpected error from Check: %s", err)
	}
	for idx, val := range msgs {
		_, fieldname := set.GetBeschreibungForID(val.OGDID)
		fmt.Printf("%d: %s [%d]: %s\n", idx, fieldname, val.OGDID, val.Text)
	}
}
