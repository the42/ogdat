package main

import (
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	"os"
	"time"
)

type DBConn struct {
	*sql.DB
}

func GetDatabaseConnection() *DBConn {

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
	return &DBConn{db}
}

func (conn *DBConn) GetLastDBHit() (*time.Time, error) {
	row := conn.QueryRow("SELECT getlasttimestamp()")

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
	_, err := conn.Exec("SELECT deleteallentries()")
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
