package main

import (
	"cgl.tideland.biz/net/atom"
	"flag"
	"fmt"
	"github.com/the42/ogdat/ogdatv21"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const AppID = "a6545f8f-e0c9-4917-83c7-3e47bd1e0247"

var logger *log.Logger

var resettdb = flag.Bool("resetdb", false, "Delete the tracking database. You will be prompted before actual deletion. Process will terminate afterwards.")
var inittdb = flag.Bool("initdb", false, "Initialize the tracking database. In case there are old entries in the tracking database, use init in conjunction with reset. Process will terminate afterwards.")
var servetdb = flag.Bool("serve", false, "Start in watchdog mode. Process will continue to run until it receives a (clean shutdown) or gets killed")
var DEBUG = flag.Bool("DEBUG", false, "DEBUG MODE")

func gotyesonprompt() bool {
	var prompt string
	fmt.Scanf("%s", &prompt)
	prompt = strings.ToLower(strings.TrimSpace(prompt))
	if len(prompt) > 0 {
		return prompt[0] == 'y'
	}
	return false
}

func getheartbeatinterval() int {

	if i, err := strconv.Atoi(os.Getenv("HEARTBEATINTERVAL")); err == nil {
		return i
	}
	return 10 // Minutes
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

	var db *DBConn
	var portal *Portal
	if *resettdb || *inittdb || *servetdb {
		// From here we need a database connection string
		db = GetDatabaseConnection(AppID)
	}
	defer db.Close()

	if *resettdb {
		logger.Println("Warning: Requesting database reset")
		fmt.Print("\n\nALL RECORDED DATA IN DATABASE  WILL BE DELETED.\nDO YOU REALLY WANT TO PROCEED? [N,y]\n")
		if !gotyesonprompt() {
			fmt.Print("\nABORTING\n\n")
			logger.Println("Info: Database reset canceled")
		} else {
			if err := db.ResetDatabase(); err != nil {
				s := fmt.Sprintf("Database reset failed: %s", err)
				fmt.Println(s)
				logger.Panic(s)
			}
		}
	}

	if *inittdb {
		if err := db.CreateDatabase(); err != nil {
			s := fmt.Sprintf("Database initialisation failed: %s", err)
			fmt.Println(s)
			logger.Panic(s)
		}
	}

	if *resettdb || *inittdb {
		logger.Println("Info: Earyl exit due to maintainance switches")
		return 2
	}

	if *DEBUG {

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
		portal = NewDataPortalAPIEndpoint(ogdatdataseturl)
		// TODO: Wrapp logic into select loop
		var processids []ogdatv21.Identifier
		heartbeatinterval := getheartbeatinterval()

		for {

			hit, err := db.GetLastDBHit()
			if err != nil {
				s := fmt.Sprintf("Cannot read last DBHit: %s", err)
				fmt.Println(s)
				logger.Panic(s)
			}

			if hit == nil {
				processids, err = portal.GetAllMetaDataIDs()
			} else {
			}

			if anzids := len(processids); anzids > 0 {
				if err := db.HeartBeat(fmt.Sprintf("%d Medadaten werden verarbeitet", anzids), StateOk); err != nil {
					panic(err)
				}

				for idx, id := range processids {

					logger.Println(fmt.Sprintf("Processing %d (%d): %v", idx, anzids, id))

					md, err := portal.GetMetadataforID(id)
					if err != nil {
						s := fmt.Sprintf("Cannot access Metadata for ID %v", id)
						fmt.Println(s)
						db.HeartBeat(s, StateFatal)
						logger.Panic(err)
					}

					messages, err := md.Check(true)
					_ = messages
				}
			}

			db.HeartBeat("Idle", StateOk)
			time.Sleep(time.Duration(heartbeatinterval) * time.Minute)
		}
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
