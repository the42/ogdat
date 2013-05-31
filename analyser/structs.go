package main

// ===================================================
// Structurs for database retrieval & redis storage
// ===================================================
type UnitDSNums struct {
	Entity  string
	Numsets int
}

type MDVersNums struct {
	MetadataVersion string
	Numsets         int
}

type Dataset struct {
	ID, CKANID  string
	Publisher   string
	Contact     string
	Description string
	Version     string
	Category    []string
}

type CKANIDUrl struct {
	CKANID string
	Url    string
}
