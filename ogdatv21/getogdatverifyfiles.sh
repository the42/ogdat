#!/usr/bin/env bash

set -e

OGDV21SPEC=http://www.ref.gv.at/uploads/media/OGD-Metadaten_2_1_2012_10.pdf
ISO639LANGFILE=http://www.loc.gov/standards/iso639-2/ISO-639-2_utf-8.txt
IANACHARSET=http://www.iana.org/assignments/character-sets/character-sets.xml

# retrieving specification from the internet
echo "Beziehen der Spezifikation"
rm `basename  $OGDV21SPEC`.*  || true
rm `basename  $OGDV21SPEC` || true
wget $OGDV21SPEC
# remove supperfluous pdfs from repeated retrieval (need no history)


PDF=`basename  $OGDV21SPEC`
TXT=`basename  $OGDV21SPEC .pdf`.txt
CSV=`basename  $OGDV21SPEC .pdf`.csv

echo "Konvertieren in Text-Datei ..."
pdftotext $PDF > $TXT

echo "Extrahieren der Information als CSV ..."
awk -f convogdatspectocsv.awk $TXT > $CSV

echo "Beziehen der ISO Sprachdatei"
rm `basename  $ISO639LANGFILE`.* || true
rm `basename  $ISO639LANGFILE` || true
wget $ISO639LANGFILE

echo "Beziehen der IANA Charset-Datei"
rm `basename  $IANACHARSET`.* || true
rm `basename  $IANACHARSET` || true
wget $IANACHARSET

XML=`basename  $IANACHARSET`
TXT=`basename  $IANACHARSET .xml`.txt
echo "Extrahieren der Information als CSV ..."
xsltproc character-sets.xsl $XML > $TXT

echo "Erfolgreich"