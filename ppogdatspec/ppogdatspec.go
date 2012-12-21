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
<style>

table {
 border-collapse:collapse;
}

th {
  text-align: left;
}

table[ogdrequired="true"] {
  background-color:hsl(30, 100%, 88%);
  border: 1px solid hsl(30, 100%, 88%);  /* orange */
}

table[ogdrequired="false"] {
  background-color:hsl(120, 100%, 88%);
  border: 1px solid hsl(120, 100%, 88%);  /* grenish */
}


table[ogdrequired="true"] th {
  background-color:hsl(30, 100%, 75%);
}

table[ogdrequired="false"] th {
  background-color: hsl(120, 100%, 75%);
}

table[ogdrequired="true"] .odd td {
  background-color:hsl(30, 100%, 95%);
}

table[ogdrequired="false"]  .odd td {
  background-color: hsl(120, 100%, 95%);
}
</style>
</head>
<body>
<div id=ogdatspecarea>{{range .Beschreibung}}
<table class="ogdatspectable" ogdrequired='{{.IsRequired}}'>
  <caption>{{.Bezeichner}} - {{if .IsRequired}}Pflichtfeld{{else}}optionaler Eintrag{{end}}</caption>
  <tbody>
    <tr>
      <th id="ID.desc.{{.ID}}">{{index $.Label 0}}</th>
      <th id="Bezeichner.desc.{{.ID}}">{{index $.Label 1}}</th>
      <th id="OGDKurzname.desc.{{.ID}}">{{index $.Label 2}}</th>
      <th id="CKANFeld.desc.{{.ID}}">{{index $.Label 3}}</th>
      <th id="Anzahl.desc.{{.ID}}">{{index $.Label 4}}</th>
    </tr>
    <tr>
      <td id="ID.item.{{.ID}}">{{.ID}}</td>
      <td id="Bezeichner.item.{{.ID}}">{{.Bezeichner}}</td>
      <td id="OGDKurzname.item.{{.ID}}">{{.OGD_Kurzname}}</td>
      <td id="CKANFeld.item.{{.ID}}">{{.CKAN_Feld}}</td>
      <td id="Anzahl.item.{{.ID}}">{{.Anzahl}}</td>
  </tbody>
  <tbody>
    <tr class="odd">
      <th id="Definition_DE.desc.{{.ID}}">{{index $.Label 5}}</th>
      <td id="Definition_DE.item.{{.ID}}" colspan="4">{{.Definition_DE}}</td>
    </tr>
    <tr class="even">
      <th id="Erlauterung.desc.{{.ID}}">{{index $.Label 6}}</th>
      <td id="Erlauterung.item.{{.ID}}" colspan="4">{{.Erlauterung}}</td>
    </tr>
    <tr class="odd">
      <th id="Beispiel.desc.{{.ID}}">{{index $.Label 7}}</th>
      <td id="Beispiel.item.{{.ID}}" colspan="4">{{.Beispiel}}</td>
    </tr>
    <tr class="even">
      <th id="ONA2270.desc.{{.ID}}">{{index $.Label 8}}</th>
      <td id="ONA2270.item.{{.ID}}" colspan="4">{{.ONA2270}}</td>
    </tr>
    <tr class="odd">
      <th id="ISO19115.desc.{{.ID}}">{{index $.Label 9}}</td>
      <td id="ISO19115.item.{{.ID}}" colspan="4">{{.ISO19115}}</td>
    </tr>
    <tr class="even">
      <th id="RDFProperty.desc.{{.ID}}">{{index $.Label 10}}</td>
      <td id="RDFProperty.item.{{.ID}}" colspan="4">{{.RDFProperty}}</td>
    </tr>
    <tr class="odd">
      <th id="Definition_EN.desc.{{.ID}}">{{index $.Label 11}}</td>
      <td id="Definition_EN.item.{{.ID}}" colspan="4">{{.Definition_EN}}</td>
    </tr>
  </tbody>
</table>{{end}}
</div>
</body>
</html>
`
