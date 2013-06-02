SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

CREATE TYPE odstatus AS ENUM (
    'updated',
    'inserted',
    'deleted',
    'error_fix',
    'warning_fix',
    'warning',
    'error',
    'info'
);

CREATE TABLE dataset (
    sysid integer NOT NULL,
    id character varying(255),
    publisher character varying(255),
    contact character varying(255),
    description text,
    vers character varying(255) NOT NULL,
    category json,
    ckanid character varying(255),
    geobbox character varying(255),
    geotoponym character varying(255)
);

CREATE SEQUENCE dataset_sysid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE dataset_sysid_seq OWNED BY dataset.sysid;

CREATE TABLE heartbeat (
    sysid integer NOT NULL,
    ts timestamp with time zone,
    statustext character varying(255),
    fetchtime timestamp with time zone,
    statuscode smallint,
    who uuid NOT NULL
);


CREATE SEQUENCE heartbeat_sysid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE heartbeat_sysid_seq OWNED BY heartbeat.sysid;

CREATE TABLE status (
    sysid integer NOT NULL,
    datasetid integer NOT NULL,
    reason_text text,
    field_id integer,
    hittime timestamp with time zone,
    status odstatus,
    fieldstatus integer
);


CREATE SEQUENCE status_sysid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE status_sysid_seq OWNED BY status.sysid;


ALTER TABLE ONLY dataset ALTER COLUMN sysid SET DEFAULT nextval('dataset_sysid_seq'::regclass);

ALTER TABLE ONLY heartbeat ALTER COLUMN sysid SET DEFAULT nextval('heartbeat_sysid_seq'::regclass);

ALTER TABLE ONLY status ALTER COLUMN sysid SET DEFAULT nextval('status_sysid_seq'::regclass);

ALTER TABLE ONLY heartbeat
    ADD CONSTRAINT pk_sysid PRIMARY KEY (sysid);

ALTER TABLE ONLY dataset
    ADD CONSTRAINT pkey PRIMARY KEY (sysid);

ALTER TABLE ONLY status
    ADD CONSTRAINT status_pkey PRIMARY KEY (sysid);

CREATE INDEX dataset_ckanid ON dataset USING btree (ckanid);

CREATE INDEX dataset_publisher ON dataset USING btree (publisher);

CREATE INDEX status_datasetid ON status USING btree (datasetid);

CREATE INDEX status_fieldstatus ON status USING btree (fieldstatus);

CREATE INDEX status_hittime ON status USING btree (hittime);

CREATE INDEX status_status ON status USING btree (status);

ALTER TABLE ONLY status
    ADD CONSTRAINT status_datasetid_fkey FOREIGN KEY (datasetid) REFERENCES dataset(sysid);
