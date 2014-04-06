package main

import (
	"time"
)

// ===================================================
// Structurs for database retrieval & redis storage
// ===================================================

type IDNums struct {
	ID      string
	Numsets int
}

type Dataset struct {
	ID, CKANID  string
	Publisher   string
	Contact     string
	Description string
	Version     string
	Category    []string
	GeoBBox     string
	GeoToponym  string
}

type CheckStatus struct {
	Reason_Text string
	FieldID     int
	Status      string
	Fieldstatus int
}

type CheckRecord struct {
	Publisher   string `redis:"-" json:"-"`
	CKANID      string
	Hittime     time.Time
	CheckStatus []CheckStatus
}

type URLCheckRecord struct {
	Publisher   string `redis:"-" json:"-"`
	CKANID      string
	Reason_Text []string
	FieldID     int
	Hittime     time.Time
}

type CKANIDTime struct {
	CKANID string
	time.Time
}

type CKANIDUrl struct {
	Publisher string
	CKANID    string
	Url       string
}
