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
	var ckanid, url *string

	for rows.Next() {
		if err := rows.Scan(&ckanid, &url); err != nil {
			return nil, err
		}

		ds := CKANIDUrl{}
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

func (conn *analyserdb) GetAN001Data() ([]CKANIDUrl, error) {
	const sqlquery = `
SELECT ckanid, reason_text
FROM status, dataset
WHERE reason_text IN (
  SELECT t.reason_text
  FROM status AS t
  WHERE t.hittime = (SELECT MAX(hittime)
                     FROM status
                     WHERE datasetid = t.datasetid
                     AND fieldstatus = (1 | x'2000'::int)
                    )
  AND fieldstatus = (1 | x'2000'::int)
  AND field_id = 14
  GROUP BY t.reason_text
  HAVING COUNT(*) > 1)
AND status.datasetid = dataset.sysid
AND status.fieldstatus = (1 | x'2000'::int)
ORDER BY reason_text`

	return conn.Getckanidurl(sqlquery)

}

func (conn *analyserdb) GetAN002Data() ([]CKANIDUrl, error) {
	const sqlquery = `
SELECT d.ckanid, t.reason_text
FROM (SELECT t.datasetid, t.reason_text
      FROM status AS t
      WHERE t.hittime = (SELECT MAX(hittime)
         FROM status
         WHERE datasetid = t.datasetid
         AND fieldstatus = (1 | x'2000'::int))
      AND t.fieldstatus = (1 | x'2000'::int)
      AND t.field_id = 14
      GROUP BY  t.reason_text, t.datasetid
      HAVING COUNT(t.datasetid) > 1) AS t, dataset AS d
WHERE d.sysid = t.datasetid`

	return conn.Getckanidurl(sqlquery)

}

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
