package main

import (
	"encoding/json"
	"github.com/the42/ogdat/database"
)

type analyserdb struct {
	database.DBConn
}

func (conn *analyserdb) GetDatasets() ([]Dataset, error) {
	const sqldatasets = `
SELECT id, ckanid, publisher, contact, description, vers, category
FROM dataset`

	rows, err := conn.Query(sqldatasets)
	if err != nil {
		return nil, err
	}

	var datasets []Dataset
	var id, ckanid, publisher, contact, description, version, scategory *string

	for rows.Next() {
		if err := rows.Scan(&id, &ckanid, &publisher, &contact, &description, &version, &scategory); err != nil {
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

func (conn *analyserdb) GetAN001Data() ([]AN001Data, error) {
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

	rows, err := conn.Query(sqlquery)
	if err != nil {
		return nil, err
	}

	var datasets []AN001Data
	var ckanid, url *string

	for rows.Next() {
		if err := rows.Scan(&ckanid, &url); err != nil {
			return nil, err
		}

		ds := AN001Data{}
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
