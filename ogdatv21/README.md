Dieses Verzeichnis enthält die Spezifiaktion der Österreichischen
Metadatenkonvetion (Open Government Data) in der Version 2.1 als CSV-Datei.

Die Spezifiaktion kann als PDF von
http://www.ref.gv.at/uploads/media/OGD-Metadaten_2_1_2012_10.pdf bezogen
werden.

Das Shell-Skrip getogdatspecv21astxt.sh bezieht dieses PDF, wandelt es in eine
Text-Datei um und extrahiert daraus eine CSV-Darstellung. Diese Werkzeuge werden
dazu benötigt:

* wget (http://www.gnu.org/software/wget/)
* pdftotext (http://en.wikipedia.org/wiki/Pdftotext)
* awk (http://www.gnu.org/software/gawk/)

Durch die Grausamkeit des PDF-Formats können allerdings drei Zeilen nicht
eindeutig extrahiert werden, wodurch die Spezifiaktion überprüft und
nachgebessert werden musste. Die bereinigte Version ist in der Datei
`ogdat_spec-2.1.csv` abgelegt.

CREDIT: Das dabei verwendete awk-Skript convogdatspectocsv.awk wurde von
http://stackoverflow.com/users/751863/steve erstellt.