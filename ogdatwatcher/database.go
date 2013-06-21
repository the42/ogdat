package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/lib/pq"
	"github.com/the42/ogdat"
	"github.com/the42/ogdat/database"
	"github.com/the42/ogdat/ogdatv21"
	"time"
)

type watcherdb struct {
	database.DBConn
}

func (conn *watcherdb) GetLastHit() (*time.Time, error) {
	// a field_id of NULL is about an inserted or updated metadata set (any may, or may have no further status records)
	row := conn.QueryRow("SELECT MAX(hittime) FROM status WHERE status != 'deleted' AND field_id IS NULL")

	var t pq.NullTime
	if err := row.Scan(&t); err != nil {
		return nil, err
	}
	if t.Valid {
		return &t.Time, nil
	}
	return nil, nil
}

type DataUrl struct {
	Url         string
	Field_id    int
	FieldStatus int
	DatasetID   database.DBID
}

var getdataurlselect = fmt.Sprintf(`
SELECT DISTINCT t.datasetid, t.field_id, t.fieldstatus, t.reason_text
FROM status AS t
WHERE t.hittime = (
  SELECT MAX(hittime)
  FROM status
  WHERE datasetid = t.datasetid
  AND fieldstatus = %d
)
AND fieldstatus = %d
ORDER BY t.datasetid`, ogdat.Info|ogdat.FetchableUrl, ogdat.Info|ogdat.FetchableUrl)

func (conn *watcherdb) GetDataUrls() ([][]DataUrl, error) {
	var dataurls [][]DataUrl
	var dataurl []DataUrl
	rows, err := conn.Query(getdataurlselect)
	if err != nil {
		return nil, err
	}

	var url string
	var field_id int
	var fieldstatus int
	var dbid database.DBID
	var olddbid database.DBID = -1

	for rows.Next() {

		if err := rows.Scan(&dbid, &field_id, &fieldstatus, &url); err != nil {
			return nil, err
		}

		if olddbid != dbid {
			if dataurl != nil {
				dataurls = append(dataurls, dataurl)
			}
			dataurl = nil
		}
		dataurl = append(dataurl, DataUrl{DatasetID: dbid, Url: url, Field_id: field_id, FieldStatus: fieldstatus})
		olddbid = dbid
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return dataurls, nil
}

func (conn *watcherdb) ResetDatabase() error {
	_, err := conn.Exec("DELETE FROM status; DELETE FROM dataset;")
	if err != nil {
		return err
	}
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func DBStringLen(in string, length int) string {
	// This is a very expensive operation. For strings len(in) >> length,
	// building by appending runes is probably cheaper
	rs := []rune(in)
	return string(rs[:min(length, len(rs))])
}

func (conn *watcherdb) InsertOrUpdateMetadataInfo(ckanid string, md *ogdatv21.MetaData) (database.DBID, bool, error) {
	// insertorupdatemetadatainfo(id character varying, pub character varying, cont character varying, descr text, vers character varying, category json, stime timestamp with time zone)
	const stmt = "SELECT * FROM insertorupdatemetadatainfo($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"

	if md == nil {
		return -1, false, fmt.Errorf("No input to process")
	}

	dbs, err := conn.Prepare(stmt)
	if err != nil {
		return -1, false, err
	}

	var id, maint *string
	if md.Metadata_Identifier != nil {
		id = new(string)
		*id = DBStringLen(md.Metadata_Identifier.String(), 255)
	}
	if md.Maintainer_Link != nil {
		maint = new(string)
		*maint = DBStringLen(md.Maintainer_Link.String(), 255)
	}

	pub := md.Publisher
	if pub != nil {
		*pub = DBStringLen(*pub, 255)
	}

	desc := md.Description

	vers := md.Schema_Name
	if vers != nil {
		*vers = DBStringLen(*vers, 255)
	}

	geobbox := md.Geographic_BBox
	if geobbox != nil {
		*geobbox = DBStringLen(*geobbox, 255)
	}

	geotoponym := md.Geographich_Toponym
	if geotoponym != nil {
		*geotoponym = DBStringLen(*geotoponym, 255)
	}

	var cats []string
	if cat := md.Categorization; cat != nil {
		for _, cat := range cat.Kategorie {
			cats = append(cats, cat.ID)
		}
	}
	cat, _ := json.Marshal(cats)

	t := time.Now().UTC()

	var sysid database.DBID
	var isnew bool
	err = dbs.QueryRow(ckanid, id, pub, maint, desc, vers, string(cat), t, geobbox, geotoponym).Scan(&sysid, &isnew)

	if err != nil {
		return -1, false, err
	}
	return sysid, isnew, nil
}

// DBID is the ID of the correspondig metadata record
func (conn *watcherdb) ProtocollCheck(id database.DBID, isnew bool, messages []ogdat.CheckMessage) error {

	// This is append only; revise later if it should also delete or update entries.
	const insstmt = "INSERT INTO status(datasetid, field_id, status, fieldstatus, reason_text, hittime) VALUES ($1, $2, $3, $4, $5, $6)"

	var stmt *sql.Stmt
	var err error
	if stmt, err = conn.Prepare(insstmt); err != nil {
		return err
	}

	// get time here and not within the loop so we have a grouping possibilitiy
	t := time.Now().UTC()
	var status string
	for _, msg := range messages {
		switch {
		case (msg.Type & ogdat.Error) != 0:
			status = "error"
		case (msg.Type & ogdat.Warning) != 0:
			status = "warning"
		case (msg.Type & ogdat.Info) != 0:
			status = "info"
		}
		if _, err = stmt.Exec(id, msg.OGDID, status, msg.Type, msg.Text, t); err != nil {
			return fmt.Errorf("Error inserting status for datasetid %d, fieldid %d: %s", id, msg.OGDID, err)
		}
	}
	return nil
}
