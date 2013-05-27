package main

import (
	"github.com/the42/ogdat/database"
)

type analyserdb struct {
	database.DBConn
}

func (conn *analyserdb) GetUnitDSNums() ([]UnitDSNums, error) {
	const sqlentityanz = `
SELECT publisher, COUNT (publisher) AS anz
FROM dataset
GROUP BY publisher
ORDER BY anz desc`

	rows, err := conn.Query(sqlentityanz)
	if err != nil {
		return nil, err
	}

	var unitsanz []UnitDSNums
	var entity string
	var anz int

	for rows.Next() {
		if err := rows.Scan(&entity, &anz); err != nil {
			return nil, err
		}
		unitsanz = append(unitsanz, UnitDSNums{Entity: entity, Numsets: anz})
	}

	return unitsanz, nil
}

func (conn *analyserdb) GetMDVersNums() ([]MDVersNums, error) {
	const sqlmdversanz = `
SELECT vers, count(vers)
FROM dataset
GROUP BY vers
ORDER BY vers`

	rows, err := conn.Query(sqlmdversanz)
	if err != nil {
		return nil, err
	}

	var mdversanz []MDVersNums
	var mdvers string
	var anz int

	for rows.Next() {
		if err := rows.Scan(&mdvers, &anz); err != nil {
			return nil, err
		}
		mdversanz = append(mdversanz, MDVersNums{MetadataVersion: mdvers, Numsets: anz})
	}

	return mdversanz, nil
}

func (conn *analyserdb) GetCategories() ([]string, error) {
	const sqlcategories = `
SELECT category
FROM dataset`

	rows, err := conn.Query(sqlcategories)
	if err != nil {
		return nil, err
	}

	var categories []string
	var cat string

	for rows.Next() {
		if err := rows.Scan(&cat); err != nil {
			return nil, err
		}
		categories = append(categories, cat)
	}

	return categories, nil
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
	var id, ckanid, publisher, contact, description, version, scategory string

	for rows.Next() {
		if err := rows.Scan(&id, &ckanid, &publisher, &contact, &description, &version, &scategory); err != nil {
			return nil, err
		}
		datasets = append(datasets, Dataset{ID: id, CKANID: ckanid, Publisher: publisher, Contact: contact, Description: description, Version: version, Category: []string{}})
	}

	return datasets, nil
}
