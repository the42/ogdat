package main

import (
	"cgl.tideland.biz/net/atom"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/lib/pq"
	ogdat "github.com/the42/ogdat/ogdatv21"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const dataseturl = "http://www.data.gv.at/katalog/api/2/rest/dataset/"
const iso639canonicallocation = "http://www.loc.gov/standards/iso639-2/ISO-639-2_utf-8.txt"
const lockfilename = ".lock.pid"

var logger *log.Logger

var resettdb = flag.Bool("resetdb", false, "Delete the tracking database. You will be prompted before actual deletion. Process will terminate afterwards.")
var inittdb = flag.Bool("initdb", false, "Initialize the tracking database. In case there are old entries in the tracking database, use init in conjunction with reset. Process will terminate afterwards.")
var initisolangs = flag.Bool("initisolangs", false, fmt.Sprintf("Download ISO-639-alpha3 code table from %s (required for checking language codes). Process will terminate afterwards.", iso639canonicallocation))
var servetdb = flag.Bool("serve", false, "Start in watchdog mode. Process will continue to run until it receives a (clean shutdown) or gets killed")
var DEBUG = flag.Bool("DEBUG", false, "DEBUG MODE")

func getdataforidentifier(id ogdat.Identifier) (*ogdat.MetaData, error) {
	resp, err := http.Get(dataseturl + id.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	bytedata, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	data := &ogdat.MetaData{}
	if err := json.Unmarshal(bytedata, data); err != nil {
		return nil, err
	}
	return data, nil
}

func getalldatasetids() ([]ogdat.Identifier, error) {

	var allsets []ogdat.Identifier
	resp, err := http.Get(dataseturl)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	bytedata, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(bytedata, &allsets); err != nil {
		return nil, err
	}
	return allsets, nil
}

func createlockfile(filename string) *os.File {
	lockfile, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_EXCL, os.FileMode(0666))
	if lockfile == nil || err != nil {
		fmt.Printf("Could not create lock file %s. Probably an instance of %s is running?\n", lockfilename, filepath.Base(os.Args[0]))
		logger.Panicln("Fatal: Lockfile creation error")
	}
	logger.Println("Info: Lockfile successfully created")
	return lockfile
}

func deletelockfile(lockfile *os.File) {
	filename := lockfile.Name()
	if err := lockfile.Close(); err != nil { // Windows want's it's file closed before unlinking
		logger.Panicln("Fatal: Can not close lockfile")
	}
	if err := os.Remove(filename); err != nil {
		logger.Panicln("Fatal: Can not delete lockfile")
	}
	logger.Println("Info: Lockfile successfully deleted")
}

func writeinfotolockfile(lockfile *os.File) {
	if err := lockfile.Truncate(0); err != nil {
		logger.Panicln("Fatal: Can not truncate lockfile")
	}
	if _, err := lockfile.Seek(0, 0); err != nil {
		logger.Panicln("Fatal: Can not seek in lockfile")
	}
	if _, err := lockfile.WriteString(fmt.Sprintf("%d:%s", os.Getpid(), time.Now())); err != nil {
		logger.Panicln("Fatal: Can not write to lockfile")
	}
	logger.Println("Info: Lockfile successfully written")
}

func getisolangfile() {

	localisofilename := filepath.Base(iso639canonicallocation)
	logger.Println("Info: requesting donwload of ISO language file")

	resp, err := http.Get(iso639canonicallocation)
	if err != nil {
		logger.Panicf("Fatal: Can not fetch ISO language file: %s\n", err)
	}
	defer resp.Body.Close()

	bytedata, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.Panicf("Fatal: While fetching ISO language file: Can not read from http stream: %s\n", err)
	}

	isofile, err := os.Create(localisofilename)
	if err != nil {
		logger.Panicf("Fatal: Can not create local ISO language file %s: %s\n", localisofilename, err)
	}
	defer isofile.Close()

	_, err = isofile.Write(bytedata)
	if err != nil {
		logger.Panicf("Fatal: Can not write to local ISO file %s: %s\n", localisofilename, err)
	}
	logger.Println("Info: ISO language file successfully downloaded")
}

func gotyesonprompt() bool {
	var prompt string
	fmt.Scanf("%s", &prompt)
	prompt = strings.ToLower(strings.TrimSpace(prompt))
	if len(prompt) > 0 {
		return prompt[0] == 'y'
	}
	return false
}

func mymain() int {

	if flag.NFlag() == 0 {
		fmt.Println("No command line flags given. Usage:")
		flag.PrintDefaults()
		logger.Panicln("Fatal: No command line flags given")
	}

	lockfile := createlockfile(lockfilename)
	defer deletelockfile(lockfile)

	writeinfotolockfile(lockfile)

	if *initisolangs {
		getisolangfile()
	}

	var db sql.DB
	if *resettdb || *inittdb || *servetdb {
		// From here we need a database connection string
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

		// TODO: { Remove test
		res, err := db.Exec("SELECT * from heartbeat")
		if err != nil {
			panic(err)
		}
		fmt.Printf("Result: %v\n", res)
		// }
	}
	defer db.Close()

	if *resettdb {
		logger.Println("Warning: Requesting database reset")
		fmt.Print("\n\nALL RECORDED DATA IN DATABASE  WILL BE DELETED.\nDO YOU REALLY WANT TO PROCEED? [N,y]\n")
		if !gotyesonprompt() {
			fmt.Print("\nABORTING\n\n")
			logger.Println("Info: Database reset canceled")
		} else {
			// TODO: Delete apporpriate tables
		}
	}

	if *inittdb {
		// TODO: add functionality for initdb
	}

	if *resettdb || *initisolangs || *inittdb {
		logger.Println("Info: Earyl exit due to maintainance switches")
		return 2
	}

	if *DEBUG {
		allsets, err := getalldatasetids()
		if err != nil {
			panic(err)
		}

		fmt.Printf("%+v\n\n", allsets)

		if len(allsets) > 0 {
			data, err := getdataforidentifier(allsets[0])
			if err != nil {
				panic(err)
			}
			fmt.Printf("%+v\n\n", data)
		}

		url, err := url.Parse("http://www.data.gv.at/katalog/revision/list?format=atom")
		if err != nil {
			panic(err)
		}
		feed, err := atom.Get(url)
		if err != nil {
			panic(err)
		}

		fmt.Printf("%+v\n\n", feed)

		onedayago := time.Now().Add(-24 * time.Hour)
		fmt.Println(onedayago)
		atomtime, err := atom.ParseTime(feed.Updated)
		if err != nil {
			panic(err)
		}

		fmt.Println(atomtime)

		if atomtime.Before(onedayago) {
			fmt.Println("Datasets have not changed")
		} else {
			fmt.Println("Datasets have changed")
		}
	}

	if *servetdb {
	}

	return 0
}

func main() {
	os.Exit(mymain())
}

func init() {
	logger = log.New(os.Stderr, filepath.Base(os.Args[0])+": ", log.LstdFlags)
	flag.Parse()
}
