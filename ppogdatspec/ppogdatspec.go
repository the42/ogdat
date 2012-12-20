package ppogdatspec

import (
	"flag"
	"github.com/the42/ogdat"
	htmltpl "html/template"
	"io"
	"log"
	"os"
	texttpl "text/template"
)

var inputfile = flag.String("if", "", "Eingabe mit OGD-Spezifikation (Standard: stdin)")
var outputfile = flag.String("if", "", "Ausgabe der Spezifikation nach (Standard: stdout)")
var templateset = flag.String("ts", "render.html.tpl", "(Satz von) Template-Dateien, die die Transformation der Spezifikation ins Ausgabeformat beschreibt")
var html = flag.Bool("html", true, "Anwendung von HTML-Escaping in der Ausgabe")
var help = flag.Bool("help", false, "Hilfe zur Verwendung")

type Templater interface {
	Execute(io.Writer, interface{}) error
}

func main() {
	flag.Parse()
	if *help {
		flag.PrintDefaults()
		return
	}

	if *inputfile == "" {
		*inputfile = os.Stdin.Name()
	}
	spec, err := ogdat.Loadogdatspec("unknown", *inputfile)
	if err != nil {
		log.Panicf("Could not load specification file %s, the error was %s\n", *inputfile, err)
	}

	var ofile *os.File
	if *outputfile == "" {
		ofile = os.Stdout
	} else {
		var err error
		ofile, err = os.OpenFile(*outputfile, os.O_RDWR|os.O_CREATE|os.O_EXCL, os.FileMode(0666))
		if err != nil {
			log.Panicf("Can't open file %s for writing, the error was: %s\n", inputfile, err)
		}
		defer ofile.Close()
	}

	var tpl Templater
	if *html {
		tpl = htmltpl.Must(htmltpl.ParseFiles(*templateset))
	} else {
		tpl = texttpl.Must(texttpl.ParseFiles(*templateset))
	}
	tpl.Execute(ofile, spec)
}
