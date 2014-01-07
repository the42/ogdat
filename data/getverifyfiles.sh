#!/usr/bin/env bash

set -e

ISO639LANGFILE=http://www.loc.gov/standards/iso639-2/ISO-639-2_utf-8.txt
IANACHARSET=http://www.iana.org/assignments/character-sets/character-sets.xml

echo "Beziehen der ISO Sprachdatei"
rm `basename  $ISO639LANGFILE`.* || true
rm `basename  $ISO639LANGFILE` || true
wget $ISO639LANGFILE

echo "Beziehen der IANA Charset-Datei"
rm `basename  $IANACHARSET`.* || true
rm `basename  $IANACHARSET` || true
wget $IANACHARSET

XML=`basename  $IANACHARSET`
IANATEXT=`basename  $IANACHARSET .xml`.txt
echo "Extrahieren der Information als TXT ..."
xsltproc character-sets.xsl $XML > $IANATEXT
tr '[:upper:]' '[:lower:]' < $IANATEXT | sort -u -o $IANATEXT

echo "Erfolgreich"