#!/usr/bin/env bash

OGDV21SPEC=http://www.ref.gv.at/uploads/media/OGD-Metadaten_2_1_2012_10.pdf

# retrieving specification from the internet
wget $OGDV21SPEC
# remove supperfluous pdfs from repeated retrieval (need no history)
rm `basename  $OGDV21SPEC`.*

PDF=`basename  $OGDV21SPEC`
TXT=`basename  $OGDV21SPEC .pdf`.txt
CSV=`basename  $OGDV21SPEC .pdf`.csv

echo "Converting to text file ..."
pdftotext $PDF > $TXT

echo "Extracting data as CSV ..."
awk -f convogdatspectocsv.awk $TXT > $CSV