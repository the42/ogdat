package main

import (
	"encoding/json"
	"fmt"
	"github.com/the42/ogdat/database"
	"time"
)

type analyserdb struct {
	database.DBConn
}

func (conn *analyserdb) GetDatasets() ([]Dataset, error) {
	const sqldatasets = `
SELECT id, ckanid, publisher, contact, description, vers, category, geobbox, geotoponym
FROM dataset`

	rows, err := conn.Query(sqldatasets)
	if err != nil {
		return nil, err
	}

	var datasets []Dataset
	var id, ckanid, publisher, contact, description, version, scategory, geobbox, geotoponym *string

	for rows.Next() {
		if err := rows.Scan(&id, &ckanid, &publisher, &contact, &description, &version, &scategory, &geobbox, &geotoponym); err != nil {
			return nil, err
		}

		ds := Dataset{}
		if id != nil {
			ds.ID = *id
		}
		if ckanid != nil {
			ds.CKANID = *ckanid
		}
		if publisher != nil {
			ds.Publisher = *publisher
		}
		if contact != nil {
			ds.Contact = *contact
		}
		if description != nil {
			ds.Description = *description
		}
		if version != nil {
			ds.Version = *version
		}
		if geobbox != nil {
			ds.GeoBBox = *geobbox
		}
		if geotoponym != nil {
			ds.GeoToponym = *geotoponym
		}
		if scategory != nil {
			var strcats []string
			if err := json.Unmarshal([]byte(*scategory), &strcats); err != nil {
				return nil, err
			}
			ds.Category = strcats
		}
		datasets = append(datasets, ds)
	}
	return datasets, nil
}

func (conn *analyserdb) Getckanidurl(query string) ([]CKANIDUrl, error) {
	rows, err := conn.Query(query)
	if err != nil {
		return nil, err
	}

	var datasets []CKANIDUrl
	var publisher, ckanid, url *string

	for rows.Next() {
		if err := rows.Scan(&publisher, &ckanid, &url); err != nil {
			return nil, err
		}

		ds := CKANIDUrl{}
		if publisher != nil {
			ds.Publisher = *publisher
		}
		if ckanid != nil {
			ds.CKANID = *ckanid
		}
		if url != nil {
			ds.Url = *url
		}
		datasets = append(datasets, ds)
	}
	return datasets, nil
}

func (conn *analyserdb) GetLastCheckResults() ([]CheckRecord, error) {
	const sqlquery = `
SELECT publisher, ckanid, outers.field_id, outers.hittime, outers.fieldstatus, outers.reason_text, outers.status
FROM status outers
INNER JOIN (select datasetid, MAX(hittime) AS hittime
  FROM status
  WHERE (fieldstatus & x'2000'::int) != x'2000'::int
  GROUP BY datasetid) as lastd
ON outers.datasetid = lastd.datasetid
  AND outers.hittime = lastd.hittime
INNER JOIN dataset
  ON dataset.sysid = outers.datasetid
WHERE NOT EXISTS (
  SELECT 1
  FROM status AS s
  WHERE s.datasetid = outers.datasetid
  AND s.status = 'deleted'
  AND s.hittime >= outers.hittime)
ORDER BY hittime DESC`

	rows, err := conn.Query(sqlquery)
	if err != nil {
		return nil, err
	}

	var checkrecord []CheckRecord
	var (
		publisher   *string
		ckanid      *string
		oldckaind   string
		field_id    *int
		t           time.Time
		fieldstatus *int
		reason_text *string
		status      *string
	)

	for rows.Next() {
		if err := rows.Scan(&publisher, &ckanid, &field_id, &t, &fieldstatus, &reason_text, &status); err != nil {
			return nil, err
		}
		if ckanid != nil && oldckaind != *ckanid {
			checkrecord = append(checkrecord, CheckRecord{Publisher: *publisher, CKANID: *ckanid, Hittime: t})
			oldckaind = *ckanid
		}

		ds := CheckStatus{Reason_Text: *reason_text, FieldID: *field_id, Status: *status, Fieldstatus: *fieldstatus}
		checkrecord[len(checkrecord)-1].CheckStatus = append(checkrecord[len(checkrecord)-1].CheckStatus, ds)
	}
	return checkrecord, nil
}

// AN001: Welche Publisher haben unterschiedliche Metadaten, die auf gleiche Daten verweisen?
func (conn *analyserdb) GetAN001Data() ([]CKANIDUrl, error) {
	const sqlquery = `
SELECT publisher, ckanid, o.reason_text
FROM status o
INNER JOIN dataset
ON dataset.sysid = o.datasetid
WHERE reason_text IN (
	SELECT reason_text -- gültige, nicht gelöschte Datensätze, die gleiche Links haben
	FROM status o
	WHERE o.field_id = 14 -- nur die felder mit resource_url
	AND o.fieldstatus = x'2001'::int -- datensätze, die von einem check importiert wurden
	AND hittime = ( -- letzer gültiger Datesatz nach einen check
	  SELECT  max(hittime)
	  FROM status
	  WHERE datasetid = o.datasetid
	  AND fieldstatus = x'2001'::int)
	AND NOT EXISTS ( -- Datensatz wurde nicht gelöscht
	  SELECT 1
	  FROM status
	  WHERE status.status = 'deleted'
	  AND o.datasetid = datasetid
	  AND hittime >= o.hittime)
	GROUP BY reason_text
	HAVING COUNT(*) > 1)
AND hittime = (
  SELECT max(hittime)
  FROM status
  WHERE o.datasetid = datasetid
  AND fieldstatus = x'2001'::int)
AND field_id = 14
AND fieldstatus = x'2001'::int
ORDER BY publisher`

	return conn.Getckanidurl(sqlquery)

}

// AN002: Welche Publisher haben Metadaten, die mehrere Ressourceeinträge haben und dabei auf gleiche Daten verweisen?
func (conn *analyserdb) GetAN002Data() ([]CKANIDUrl, error) {
	const sqlquery = `
SELECT publisher, ckanid, reason_text
FROM dataset
INNER JOIN (
  SELECT datasetid, reason_text
  FROM status AS t
  WHERE hittime = ( -- letzter aktueller Datensatz
    SELECT MAX(hittime)
    FROM status
    WHERE datasetid = t.datasetid
    AND fieldstatus = (1 | x'2000'::int))
  AND NOT EXISTS ( -- Datensatz wurde noch nicht gelöscht
    SELECT 1
    FROM status
    WHERE status.status = 'deleted'
    AND status.datasetid = t.datasetid
    AND status.hittime >= t.hittime)
  AND t.fieldstatus = (1 | x'2000'::int) -- nur das URL-Infofeld
  AND t.field_id = 14 -- nur das Metadatenfeld #14 (resource_url)
  GROUP BY t.datasetid, t.reason_text
  HAVING COUNT(t.reason_text) > 1) AS t
ON t.datasetid = dataset.sysid
ORDER BY publisher`

	return conn.Getckanidurl(sqlquery)

}

// BS001: Die letzten num Änderungen mit CKANID und Datum
func (conn *analyserdb) GetBS001Data(num int) ([]CKANIDTime, error) {
	sqlquery := fmt.Sprintf(`
SELECT ckanid, hittime
FROM dataset
INNER JOIN status
ON status.datasetid = dataset.sysid
AND status = 'updated'
ORDER BY hittime DESC
LIMIT %d`, num)

	rows, err := conn.Query(sqlquery)
	if err != nil {
		return nil, err
	}

	var datasets []CKANIDTime
	var ckanid *string
	var t time.Time

	for rows.Next() {
		if err := rows.Scan(&ckanid, &t); err != nil {
			return nil, err
		}

		ds := CKANIDTime{Time: t}
		if ckanid != nil {
			ds.CKANID = *ckanid
		}
		datasets = append(datasets, ds)
	}
	return datasets, nil
}
