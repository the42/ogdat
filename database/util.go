package database

import (
	"database/sql"
	"fmt"
	"github.com/lib/pq"
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
	Appid string
}

type State int16

const (
	StateOk State = iota + 1
	StateWarning
	StateError
	StateFatal
)

type DBID int32

func GetDatabaseConnection() (*sql.DB, error) {

	var dburl, dbconnstring string

	if dburl = os.Getenv("DATABASE_URL"); dburl == "" {
		dburl = "postgres://"
	}

	dbconnstring, err := pq.ParseURL(dburl)
	if err != nil {
		return nil, fmt.Errorf("Invalid Database Url: %s (%s)\n", dburl, err)
	}

	db, err := sql.Open("postgres", dbconnstring)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (conn *DBConn) HeartBeat() error {
	const (
		updatestmt = "UPDATE heartbeat SET ts=$1 WHERE who=$2 AND sysid=$3"
		insertstmt = "INSERT INTO heartbeat(ts, statuscode, statustext, who) VALUES($1, 0, 'Alive', $2)"
	)

	var hbstatement *sql.Stmt
	var sysid DBID

	err := conn.QueryRow("SELECT asi.sysid FROM (SELECT sysid, ts, who, MAX(ts) OVER (PARTITION BY who) max_ts FROM heartbeat) asi WHERE asi.ts = max_ts AND who=$1", conn.Appid).Scan(&sysid)

	switch {
	case err == sql.ErrNoRows:
		hbstatement, err = conn.Prepare(insertstmt)
		_, err = hbstatement.Exec(time.Now().UTC(), conn.Appid)
	case err != nil:
		return fmt.Errorf("Error heartbeating database: %s", err)
	default:
		hbstatement, err = conn.Prepare(updatestmt)
		_, err = hbstatement.Exec(time.Now().UTC(), conn.Appid, sysid)
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

	err := conn.QueryRow("SELECT asi.statuscode, asi.sysid FROM (SELECT sysid, ts, statuscode, who, MAX(ts) OVER (PARTITION BY who) max_ts FROM heartbeat) asi WHERE asi.ts = max_ts AND who=$1", conn.Appid).Scan(&statuscode, &sysid)

	switch {
	case err == sql.ErrNoRows:
		hbstatement, err = conn.Prepare(insertstmt)
		_, err = hbstatement.Exec(time.Now().UTC(), code, message, conn.Appid)
	case err != nil:
		return fmt.Errorf("Error reading last DBLog status code: %s", err)
	case statuscode != StateOk && replacelatest:
		return fmt.Errorf("Last DBLog caused a non-ok state and update requested, doing nothing")
	case replacelatest:
		hbstatement, err = conn.Prepare(updatestmt)
		_, err = hbstatement.Exec(time.Now().UTC(), code, message, conn.Appid, sysid)
	default:
		hbstatement, err = conn.Prepare(insertstmt)
		_, err = hbstatement.Exec(time.Now().UTC(), code, message, conn.Appid)
	}
	defer hbstatement.Close()

	if err != nil {
		return fmt.Errorf("Error executing DBLog: %s", err)
	}
	return nil
}
