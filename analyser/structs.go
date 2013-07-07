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

type CKANIDUrl struct {
	CKANID string
	Url    string
}

type CKANIDTime struct {
	CKANID string
	time.Time
}
