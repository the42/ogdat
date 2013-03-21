package main

import (
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	"os"
)

func getDatabaseConnection() *sql.DB {
	
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
