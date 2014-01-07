#!/usr/bin/env bash

set -e

OGDSPEC=http://reference.e-government.gv.at/uploads/media/OGD-Metadaten_2_2_2013_12_12.pdf

# retrieving specification from the internet
echo "Beziehen der Spezifikation"
rm `basename  $OGDSPEC`.*  || true
rm `basename  $OGDSPEC` || true
wget $OGDSPEC
# remove supperfluous pdfs from repeated retrieval (need no history)


PDF=`basename  $OGDSPEC`
TXT=`basename  $OGDSPEC .pdf`.txt
CSV=`basename  $OGDSPEC .pdf`.csv

echo "Konvertieren in Text-Datei ..."
pdftotext $PDF > $TXT

echo "Extrahieren der Information als CSV ..."
awk -f convogdatspectocsv.awk $TXT > $CSV
echo "Skript Ende"