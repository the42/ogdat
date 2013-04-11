CREATE TYPE odstatus AS ENUM (
    'updated',
    'inserted',
    'deleted',
    'error_fix',
    'warning_fix',
    'warning',
    'error'
);

CREATE TABLE dataset (
    sysid integer NOT NULL,
    id character varying(255) NOT NULL,
    publisher character varying(255),
    contact character varying(255) NOT NULL,
    description text,
    vers character varying(255) NOT NULL,
    category json
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
    status odstatus
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

CREATE INDEX dataset_publisher ON dataset USING btree (publisher);

CREATE INDEX status_hittime ON status USING btree (hittime);

CREATE INDEX status_status ON status USING btree (status);

ALTER TABLE ONLY status
    ADD CONSTRAINT status_datasetid_fkey FOREIGN KEY (datasetid) REFERENCES dataset(sysid);
