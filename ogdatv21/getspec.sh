#!/usr/bin/env bash

set -e

OGDSPEC=http://www.ref.gv.at/uploads/media/OGD-Metadaten_2_1_2012_10.pdf

echo "Beziehen der Spezifikation"
# remove supperfluous pdfs from repeated retrieval (need no history)
rm `basename  $OGDSPEC`.*  || true
rm `basename  $OGDSPEC` || true
# retrieving specification from the internet
wget $OGDSPEC

PDF=`basename  $OGDSPEC`
TXT=`basename  $OGDSPEC .pdf`.txt
CSV=`basename  $OGDSPEC .pdf`.csv

echo "Konvertieren in Text-Datei ..."
pdftotext $PDF > $TXT

echo "Extrahieren der Information als CSV ..."
awk -f convogdatspectocsv.awk $TXT > $CSV

echo "Ende"