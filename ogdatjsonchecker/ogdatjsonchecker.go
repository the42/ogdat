package main

import (
	"flag"
	"github.com/the42/ogdat/ogdatv21"
)

var mdsource = flag.String("if", "", "Einzelne, CKAN-compatible, JSON-Beschreibung eines Metadatensatzes. Kann eine lokale Datei sein, oder über http/https bezogen werden. Standard: stdin")
var version = flag.String("version", ogdatv21.Version, "Version, nach der das OGD Metadatendokument überprüft werden soll")

func main() {
	flag.Parse()

	if *mdsource == "" {
	} else {
		// url.Parse; wenn Ressource == http, http.Get sonst open
	}
}
