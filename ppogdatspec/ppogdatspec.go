package main

import (
	"flag"
	"fmt"
	"github.com/the42/ogdat"
	htmltpl "html/template"
	"io"
	"log"
	"os"
	texttpl "text/template"
)

var inputfile = flag.String("if", "", "Eingabe mit OGD-Spezifikation (Standard: stdin)")
var outputfile = flag.String("of", "", "Ausgabe der Spezifikation nach (Standard: stdout)")
var templateset = flag.String("ts", "", "(Satz von) Template-Dateien, die die Transformation der Spezifikation ins Ausgabeformat beschreibt")
var html = flag.Bool("html", true, "Anwendung von HTML-Escaping in der Ausgabe")
var printbuiltin = flag.Bool("printbuiltin", false, "Ausgabe von eingebautem Template (stdout)")
var help = flag.Bool("help", false, "Hilfe zur Verwendung")

type Templater interface {
	Execute(io.Writer, interface{}) error
}

func main() {
	flag.Parse()

	// Display help and exit
	if *help {
		flag.PrintDefaults()
		return
	}

	// Display default template and exit
	if *printbuiltin {
		fmt.Print(builtintpl)
		return
	}

	// If no input spec file is given, read from stdin
	if *inputfile == "" {
		*inputfile = os.Stdin.Name()
	}
	spec, err := ogdat.Loadogdatspec("unknown", *inputfile)
	if err != nil {
		log.Panicf("Could not load specification file %s, the error was %s\n", *inputfile, err)
	}

	var tpl Templater
	if *templateset != "" { // If a template set was given, try to read it
		if *html {
			tpl = htmltpl.Must(htmltpl.ParseFiles(*templateset))
		} else {
			tpl = texttpl.Must(texttpl.ParseFiles(*templateset))
		}
	} else { // otherwise use the built-in
		tpl = htmltpl.Must(htmltpl.New("").Parse(builtintpl))
	}

	// Open the output file
	var ofile *os.File
	if *outputfile == "" {
		ofile = os.Stdout
	} else {
		var err error
		// Do not overwrite an exisiting file but fail
		ofile, err = os.OpenFile(*outputfile, os.O_RDWR|os.O_CREATE|os.O_EXCL, os.FileMode(0666))
		if err != nil {
			log.Panicf("Can't open file %s for writing, the error was: %s\n", inputfile, err)
		}
		defer ofile.Close()
	}
	if err := tpl.Execute(ofile, spec); err != nil {
		log.Panicf("Template execution failed: %s\n", err)
	}
	log.Printf("Sepcification successfully transformed\n")
}

const builtintpl = `
<!DOCTYPE html>
<html lang="de">
<head>
  <title></title>
  <meta charset="UTF-8">
</head>
<body>
<div id=ogdspecarea>{{range .Beschreibung}}
<table class="ogdatspectable[required='{{.IsRequired}}']">
  <caption></caption>
  <tbody>
    <tr>
      <td class="ogddatspectableitem[description='true']" id="ID.desc.{{.ID}}">{{index $.Label 0}}</td>
      <td class="ogddatspectableitem[description='true']" id="Bezeichner.desc.{{.ID}}">{{index $.Label 1}}</td>
      <td class="ogddatspectableitem[description='true']" id="OGD_Kurzname.desc.{{.ID}}">{{index $.Label 2}}</td>
      <td class="ogddatspectableitem[description='true']" id="CKAN_Feld.desc.{{.ID}}">{{index $.Label 3}}</td>
      <td class="ogddatspectableitem[description='true']" id="Anzahl.desc.{{.ID}}">{{index $.Label 4}}</td>
    </tr>
    <tr>
      <td class="ogddatspectableitem[description='false']" id="ID.item.{{.ID}}">{{.ID}}</td>
      <td class="ogddatspectableitem[description='false']" id="Bezeichner.item.{{.ID}}">{{.Bezeichner}}</td>
      <td class="ogddatspectableitem[description='false']" id="OGD_Kurzname.item.{{.ID}}">{{.OGD_Kurzname}}</td>
      <td class="ogddatspectableitem[description='false']" id="CKAN_Feld.item.{{.ID}}">{{.CKAN_Feld}}</td>
      <td class="ogddatspectableitem[description='false']" id="Anzahl.item.{{.ID}}">{{.Anzahl}}</td>
    <tr>
    </tr>
  </tbody>
  <tbody>
    <tr>
      <td class="ogddatspectableitem[description='true']" id="Definition_DE.desc.{{.ID}}">{{index $.Label 5}}</td>
      <td class="ogddatspectableitem[description='false']" id="Definition_DE.item.{{.ID}}">{{.Definition_DE}}</td>
    </tr>
    <tr>
      <td class="ogddatspectableitem[description='true']" id="Erlauterung.desc.{{.ID}}">{{index $.Label 6}}</td>
      <td class="ogddatspectableitem[description='false']" id="Erlauterung.item.{{.ID}}">{{.Erlauterung}}</td>
    </tr>
    <tr>
      <td class="ogddatspectableitem[description='true']" id="Beispiel.desc.{{.ID}}">{{index $.Label 7}}</td>
      <td class="ogddatspectableitem[description='false']" id="Beispiel.item.{{.ID}}">{{.Beispiel}}</td>
    </tr>
    <tr>
      <td class="ogddatspectableitem[description='true']" id="ONA2270.desc.{{.ID}}">{{index $.Label 8}}</td>
      <td class="ogddatspectableitem[description='false']" id="ONA2270.item.{{.ID}}">{{.ONA2270}}</td>
    </tr>
    <tr>
      <td class="ogddatspectableitem[description='true']" id="ISO19115.desc.{{.ID}}">{{index $.Label 9}}</td>
      <td class="ogddatspectableitem[description='false']" id="ISO19115.item.{{.ID}}">{{.ISO19115}}</td>
    </tr>
    <tr>
      <td class="ogddatspectableitem[description='true']" id="RDFProperty.desc.{{.ID}}">{{index $.Label 10}}</td>
      <td class="ogddatspectableitem[description='false']" id="RDFProperty.item.{{.ID}}">{{.RDFProperty}}</td>
    </tr>
    <tr>
      <td class="ogddatspectableitem[description='true']" id="Definition_EN.desc.{{.ID}}">{{index $.Label 11}}</td>
      <td class="ogddatspectableitem[description='false']" id="Definition_EN.item.{{.ID}}">{{.Definition_EN}}</td>
    </tr>
  </tbody>
</table>{{end}}
</div>
</body>
</html>
`
