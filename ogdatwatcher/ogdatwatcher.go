package main

import (
	"cgl.tideland.biz/net/atom"
	"flag"
	"fmt"
	"github.com/the42/ogdat/ckan"
	"github.com/the42/ogdat/ogdatv21"
	"github.com/the42/ogdat/schedule"
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
var db *DBConn
var portal *ckan.Portal

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

	if i, err := strconv.Atoi(os.Getenv("HEARTBEAT_INTERVAL")); err == nil {
		return i
	}
	return 10 // Minutes
}

func getnumworkers() int {
	return 4
}

func getckanurl() (url string) {

	const CKAN_URL = "http://www.data.gv.at/katalog/api/"

	url = os.Getenv("CKAN_URL")
	if url == "" {
		url = CKAN_URL
	}
	return
}

func initdb() {
	if err := db.CreateDatabase(); err != nil {
		s := fmt.Sprintf("Database initialisation failed: %s", err)
		fmt.Println(s)
		logger.Panic(s)
	}
}

func resetdb() {
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

func stringslicetoiface(ss []string) []interface{} {
					slice := make([]interface{}, len(ss))
				for i, v := range ss {
					slice[i] = v
				}
				return slice
}

func ifaceslicetostring(ifs []interface{}) []string {
					slice := make([]string, len(ifs))
					for i, v := range ifs {
						s, ok := v.(string)
						if !ok {
							panic("Interface value not of string type")
						}
						slice[i] = s
					}
					return slice
}


func processmetadataids(conn DBer, processids []string) (string, error) {

	for _, id := range processids {

		logger.Println(fmt.Sprintf("Processing %v", id))

		mdjson, err := portal.GetJSONforID(id, true)
		if err != nil {
			return fmt.Sprintf("Cannot fetch JSON for ID %v", id), err
		}

		md, err := ogdatv21.MetadatafromJSON(mdjson)
		if err != nil {
			return fmt.Sprintf("Cannot access Metadata for ID %v", id), err
		}

		dbdatasetid, err := db.InsertOrUpdateMetadataInfo(md)
		if err != nil {
			return fmt.Sprintf("Database Error: %v", id), err
		}

		messages, err := md.Check(true)
		if err != nil {
			return fmt.Sprintf("Metadata Check Error: %v", id), err
		}

		if err = db.ProtocollCheck(dbdatasetid, messages); err != nil {
			return fmt.Sprintf("Metadata Check Error: %v", id), err
		}
	}
	return "", nil
}

func mymain() int {

	if flag.NFlag() == 0 {
		fmt.Println("No command line flags given. Usage:")
		flag.PrintDefaults()
		logger.Panicln("Fatal: No command line flags given")
	}

	lockfile := NewLockfile(lockfilename)
	defer lockfile.Delete()
	lockfile.WriteInfo()

	dbconnection := GetDatabaseConnection(AppID)
	db = &DBConn{dbconnection, AppID}
	defer dbconnection.Close()

	if *resettdb || *inittdb {
		if *inittdb {
			initdb()
		}
		if *resettdb {
			resetdb()
		}
		logger.Println("Info: Earyl exit due to maintainance switches")
		return 2
	}

	if *servetdb {

		portal = ckan.NewDataPortalAPIEndpoint(getckanurl(), "2/")
		heartbeatinterval := getheartbeatinterval()
		numworkers := getnumworkers()

		for {
			hit, err := db.GetLastHit()
			if err != nil {
				s := fmt.Sprintf("Cannot read last DBHit: %s", err)
				fmt.Println(s)
				logger.Panic(s)
			}

			var processids []string
			if hit == nil {
				processids, err = portal.GetAllMetaDataIDs()
			} else {
			}

			if anzids := len(processids); anzids > 0 {

				tx, _ := dbconnection.Begin()
				f := func(slice []interface{}) {
					if s, err := processmetadataids(tx, ifaceslicetostring(slice)); err != nil {
						fmt.Println(s)
						logger.Panic(s)
					}
				}

				db.LogMessage(fmt.Sprintf("%d Medadaten werden verarbeitet", anzids), StateOk, true)
				finish := schedule.Schedule(stringslicetoiface(processids), numworkers, f)

				select {
				case <-finish:
					tx.Commit()
					db.LogMessage("Idle", StateOk, true)
				case <-time.After(time.Duration(heartbeatinterval) * time.Minute):
					db.HeartBeat()
				}
			}
		}
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
	return 0
}

func main() {
	os.Exit(mymain())
}

func init() {
	logger = log.New(os.Stderr, filepath.Base(os.Args[0])+": ", log.LstdFlags)
	flag.Parse()
}
