package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/lib/pq"
	"github.com/the42/ogdat"
	"github.com/the42/ogdat/ogdatv21"
	"os"
	"time"
)

type DBer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Prepare(query string) (*sql.Stmt, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

type DBConn struct {
	DBer
	appid string
}

type State int16

const (
	StateOk State = iota + 1
	StateWarning
	StateError
	StateFatal
)

type DBID int32

func GetDatabaseConnection() *sql.DB {

	var dburl, dbconnstring string

	if dburl = os.Getenv("DATABASE_URL"); dburl == "" {
		dburl = "postgres://"
	}

	dbconnstring, err := pq.ParseURL(dburl)
	if err != nil {
		fmt.Printf("Invalid Database Url: %s\n", dburl)
		logger.Panicf("Fatal: Invalid Database Url: %s\n", dburl)
	}

	db, err := sql.Open("postgres", dbconnstring)
	if err != nil {
		fmt.Println("Unable to connect to dabase")
		logger.Panicln("Unable to connect to dabase")
	}
	return db
}

func (conn *DBConn) GetLastHit() (*time.Time, error) {
	row := conn.QueryRow("SELECT MAX(hittime) FROM status WHERE status != 'deleted'")

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
	DatasetID   DBID
}

var getdataurlselect = fmt.Sprintf(`SELECT t.datasetid, t.field_id, t.fieldstatus, t.reason_text
FROM status AS t
WHERE t.hittime = (SELECT MAX(hittime)
FROM status
WHERE datasetid = t.datasetid)
AND fieldstatus = %d
ORDER BY t.datasetid`, ogdat.Info|ogdat.FetchableUrl)

func (conn *DBConn) GetDataUrls() ([][]DataUrl, error) {
	var dataurls [][]DataUrl
	var dataurl []DataUrl
	rows, err := conn.Query(getdataurlselect)
	if err != nil {
		return nil, err
	}

	var url string
	var field_id int
	var fieldstatus int
	var dbid DBID
	var olddbid DBID = -1

	for rows.Next() {

		if err := rows.Scan(&dbid, &field_id, &fieldstatus, &url); err != nil {
			return nil, err
		}

		if olddbid != dbid {
			dataurls = append(dataurls, dataurl)
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

func (conn *DBConn) ResetDatabase() error {
	_, err := conn.Exec("DELETE FROM status; DELETE FROM dataset;")
	if err != nil {
		return err
	}
	return nil
}

// Deliberately use no stored procedures
func (conn *DBConn) HeartBeat() error {
	const (
		updatestmt = "UPDATE heartbeat SET ts=$1 WHERE who=$2 AND sysid=$3"
		insertstmt = "INSERT INTO heartbeat(ts, statuscode, statustext, who) VALUES($1, 0, 'Alive', $2)"
	)

	var hbstatement *sql.Stmt
	var sysid DBID

	err := conn.QueryRow("SELECT asi.sysid FROM (SELECT sysid, ts, who, MAX(ts) OVER (PARTITION BY who) max_ts FROM heartbeat) asi WHERE asi.ts = max_ts AND who=$1", conn.appid).Scan(&sysid)

	switch {
	case err == sql.ErrNoRows:
		hbstatement, err = conn.Prepare(insertstmt)
		_, err = hbstatement.Exec(time.Now().UTC(), conn.appid)
	case err != nil:
		return fmt.Errorf("Error heartbeating database: %s", err)
	default:
		hbstatement, err = conn.Prepare(updatestmt)
		_, err = hbstatement.Exec(time.Now().UTC(), conn.appid, sysid)
	}
	defer hbstatement.Close()

	if err != nil {
		return fmt.Errorf("Error executing heartbeat: %s", err)
	}
	return nil
}

// Deliberately use no stored procedures
func (conn *DBConn) LogMessage(message string, code State, replacelatest bool) error {

	const (
		updatestmt = "UPDATE heartbeat SET ts=$1, statuscode=$2, statustext=$3 WHERE who=$4 AND sysid=$5"
		insertstmt = "INSERT INTO heartbeat(ts, statuscode, statustext, who) VALUES($1, $2, $3, $4)"
	)

	var hbstatement *sql.Stmt
	var statuscode State
	var sysid DBID

	err := conn.QueryRow("SELECT asi.statuscode, asi.sysid FROM (SELECT sysid, ts, statuscode, who, MAX(ts) OVER (PARTITION BY who) max_ts FROM heartbeat) asi WHERE asi.ts = max_ts AND who=$1", conn.appid).Scan(&statuscode, &sysid)

	switch {
	case err == sql.ErrNoRows:
		hbstatement, err = conn.Prepare(insertstmt)
		_, err = hbstatement.Exec(time.Now().UTC(), code, message, conn.appid)
	case err != nil:
		return fmt.Errorf("Error reading last DBLog status code: %s", err)
	case statuscode != StateOk && replacelatest:
		return fmt.Errorf("Last DBLog caused a non-ok state and update requested, doing nothing")
	case replacelatest:
		hbstatement, err = conn.Prepare(updatestmt)
		_, err = hbstatement.Exec(time.Now().UTC(), code, message, conn.appid, sysid)
	default:
		hbstatement, err = conn.Prepare(insertstmt)
		_, err = hbstatement.Exec(time.Now().UTC(), code, message, conn.appid)
	}
	defer hbstatement.Close()

	if err != nil {
		return fmt.Errorf("Error executing DBLog: %s", err)
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

func (conn *DBConn) InsertOrUpdateMetadataInfo(ckanid string, md *ogdatv21.MetaData) (DBID, bool, error) {
	// insertorupdatemetadatainfo(id character varying, pub character varying, cont character varying, descr text, vers character varying, category json, stime timestamp with time zone)
	const stmt = "SELECT * FROM insertorupdatemetadatainfo($1, $2, $3, $4, $5, $6, $7, $8)"

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

	var cats []string
	if cat := md.Categorization; cat != nil {
		for _, cat := range cat.Kategorie {
			cats = append(cats, cat.ID)
		}
	}
	cat, _ := json.Marshal(cats)

	t := time.Now().UTC()

	var sysid DBID
	var isnew bool
	err = dbs.QueryRow(ckanid, id, pub, maint, desc, vers, string(cat), t).Scan(&sysid, &isnew)

	if err != nil {
		return -1, false, err
	}
	return sysid, isnew, nil
}

// DBID is the ID of the correspondig metadata record
func (conn *DBConn) ProtocollCheck(id DBID, isnew bool, messages []ogdat.CheckMessage) error {

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
