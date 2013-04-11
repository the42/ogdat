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

func (conn *DBConn) ResetDatabase() error {
	_, err := conn.Exec("DELETE FROM status; DELETE FROM dataset;")
	if err != nil {
		return err
	}
	return nil
}

func (conn *DBConn) CreateDatabase() error {
	_, err := conn.Exec(postgresdbcreatestatement)
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

func (conn *DBConn) InsertOrUpdateMetadataInfo(md *ogdatv21.MetaData) (DBID, bool, error) {
	// insertorupdatemetadatainfo(id character varying, pub character varying, cont character varying, descr text, vers character varying, category json, stime timestamp with time zone)
	const stmt = "SELECT * FROM insertorupdatemetadatainfo($1, $2, $3, $4, $5, $6, $7)"

	if md == nil {
		return -1, false, fmt.Errorf("No input to process")
	}

	dbs, err := conn.Prepare(stmt)
	if err != nil {
		return -1, false, err
	}

	id := DBStringLen(md.Metadata_Identifier.String(), 255)
	maint := DBStringLen(md.Maintainer_Link.String(), 255)

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
	err = dbs.QueryRow(id, pub, maint, desc, vers, string(cat), t).Scan(&sysid, &isnew)

	if err != nil {
		return -1, false, err
	}
	return sysid, isnew, nil
}

// DBID is the ID of the correspondig metadata record
func (conn *DBConn) ProtocollCheck(id DBID, isnew bool, messages []ogdat.CheckMessage) error {

	// This is append only; revise later if it should also delete or update entries.
	const insstmt = "INSERT INTO status(datasetid, field_id, status, reason_text, hittime) VALUES ($1, $2, $3, $4, $5)"

	var stmt *sql.Stmt
	var err error
	if stmt, err = conn.Prepare(insstmt); err != nil {
		return err
	}

	// get time here and not within the loop so we have a grouping possibilitiy
	t := time.Now().UTC()
	var status string
	for _, msg := range messages {
		switch msg.Type {
		case ogdat.Error, ogdat.StructuralError:
			status = "error"
		default:
			status = "warning"
		}
		if _, err = stmt.Exec(id, msg.OGDID, status, msg.Text, t); err != nil {
			return fmt.Errorf("Error inserting status for datasetid %d, fieldid %d: %s", id, msg.OGDID, err)
		}
	}
	return nil
}

const postgresdbcreatestatement = `
SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;
CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;
CREATE TYPE odcategory AS ENUM (
    'arbeit',
    'bevölkerung',
    'bildung-und-forschung',
    'finanzen-und-rechnungswesen',
    'geographie-und-planung',
    'gesellschaft-und-soziales',
    'gesundheit',
    'kunst-und-kultur',
    'land-und-forstwirtschaft',
    'sport-und-freizeit',
    'umwelt',
    'verkehr-und-technik',
    'verwaltung-und-politik',
    'wirtschaft-und-tourismus'
);
CREATE TYPE odstatus AS ENUM (
    'updated',
    'inserted',
    'deleted',
    'error_fix',
    'warning_fix',
    'warning',
    'error'
);

CREATE FUNCTION deleteallentries() RETURNS void
    LANGUAGE sql
    AS $$
delete from status;
-- insert into status(reason_text) values('Hallo');
delete from dataset;
$$;

CREATE FUNCTION getlasttimestamp() RETURNS timestamp with time zone
    LANGUAGE sql
    AS $$select max(hittime) from status;$$;

SET default_tablespace = '';

SET default_with_oids = false;

CREATE TABLE dataset (
    sysid integer NOT NULL,
    id character varying(255) NOT NULL,
    publisher character varying(255),
    contact character varying(255) NOT NULL,
    description text,
    version character varying(20) DEFAULT 'v1'::character varying NOT NULL,
    category json
);

CREATE SEQUENCE dataset_sysid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE dataset_sysid_seq OWNED BY dataset.sysid;

SELECT pg_catalog.setval('dataset_sysid_seq', 1, true);

CREATE TABLE heartbeat (
    sysid integer NOT NULL,
    "when" timestamp with time zone,
    statustext character varying(255),
    fetchtime timestamp with time zone,
    statuscode smallint
);

CREATE SEQUENCE heartbeat_sysid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE heartbeat_sysid_seq OWNED BY heartbeat.sysid;

SELECT pg_catalog.setval('heartbeat_sysid_seq', 1, false);

CREATE TABLE status (
    sysid integer NOT NULL,
    datasetid integer NOT NULL,
    reason_text character varying(255),
    field_id integer,
    hittime timestamp with time zone,
    status odstatus
);

CREATE SEQUENCE status_sysid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE status_sysid_seq OWNED BY status.sysid;

SELECT pg_catalog.setval('status_sysid_seq', 1, true);


ALTER TABLE ONLY dataset ALTER COLUMN sysid SET DEFAULT nextval('dataset_sysid_seq'::regclass);

ALTER TABLE ONLY heartbeat ALTER COLUMN sysid SET DEFAULT nextval('heartbeat_sysid_seq'::regclass);

ALTER TABLE ONLY status ALTER COLUMN sysid SET DEFAULT nextval('status_sysid_seq'::regclass);


ALTER TABLE ONLY heartbeat
    ADD CONSTRAINT pk_sysid PRIMARY KEY (sysid);

ALTER TABLE ONLY dataset
    ADD CONSTRAINT pkey PRIMARY KEY (sysid);

ALTER TABLE ONLY status
    ADD CONSTRAINT status_pkey PRIMARY KEY (sysid);

ALTER TABLE ONLY status
    ADD CONSTRAINT status_datasetid_fkey FOREIGN KEY (datasetid) REFERENCES dataset(sysid);
`
