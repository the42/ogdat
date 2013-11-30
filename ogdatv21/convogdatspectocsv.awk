# AWK file credit to http://stackoverflow.com/users/751863/steve

BEGIN {
    RS="\nID\n"
    FS="\n"
    OFS="|"

    required["1"]; required["5"]; required["8"]; required["9"]; required["10"];
    required["11"]; required["14"]; required["15"]; required["19"]; required["21"]; required["24"];

    headpart = "Bezeichner|OGD-Kurzname|CKAN Feld|Anzahl|Definition|Erläuterung|Beispiel|ON A 2270:2010|ON/EN/ISO 19115:2003|RDF property|Definition Englisch"
    ns = split(headpart, b, OFS)
    for (i=1; i <= ns; i++) {
        a[b[i]]
    }
    print "ID" OFS headpart OFS "Occurence"
}

NR>2 {
    for (i=1;i<=NF;i++) {

        l = (l ? l (l ~ /^(extras|resources):$/ ? "" : " ") : "") $i

        if ($(i+1) == "" ) {
            if (l in a) {
                l = ""
            }
            else if (l != "") {
                c = (c ? c OFS : "") l
                n++
                if (n == 1) {
                    id = $i
                }
                l = ""
            }
        }
        if (n == 12) {
            if (c ~ /^[0-9]/) {
                if (id in required) {
                    c = c OFS "R"
                } else {
                    c = c OFS "O"
                }
            } else {
                c = "ERROR: BAD RECORD"
            }
            print c
            c = n = ""
            next
        }
    }
}