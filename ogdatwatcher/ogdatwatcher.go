package main

import (
	"flag"
	"fmt"
	"github.com/the42/ogdat/ckan"
	"github.com/the42/ogdat/ogdatv21"
	"github.com/the42/ogdat/schedule"
	"log"
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
	if i, err := strconv.Atoi(os.Getenv("PARALLEL_FETCHNO")); err == nil {
		return i
	}
	return 4 // process four IDs in parallel
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

func processmetadataids(conn *DBConn, processids []string) error {

	for _, id := range processids {

		logger.Println(fmt.Sprintf("Processing %v", id))

		mdjson, err := portal.GetJSONforID(id, true)
		if err != nil {
			return fmt.Errorf("Cannot fetch JSON for ID %v: %s", id, err)
		}

		md, err := ogdatv21.MetadatafromJSON(mdjson)
		if err != nil {
			return fmt.Errorf("Cannot access Metadata for ID %v: %s", id, err)
		}

		dbdatasetid, isnew, err := conn.InsertOrUpdateMetadataInfo(md)
		if err != nil {
			return fmt.Errorf("InsertOrUpdateMetadataInfo: Database Error at id %v: %s", id, err)
		}

		messages, err := md.Check(true)
		if err != nil {
			return fmt.Errorf("Metadata Check Error for id %v: %s", id, err)
		}

		if err = conn.ProtocollCheck(dbdatasetid, isnew, messages); err != nil {
			return fmt.Errorf("ProtocollCheck: Database Error at id %v: %s", id, err)
		}
	}
	return nil
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
				processids, err = portal.GetChangedPackageIDsSince(*hit, numworkers)
			}

			if anzids := len(processids); anzids > 0 {

				tx, _ := dbconnection.Begin()
				scheduler := schedule.New(numworkers)
				conn := &DBConn{DBer: tx, appid: AppID}
				f := func(slice []interface{}) error {
					if err := processmetadataids(conn, ifaceslicetostring(slice)); err != nil {
						return err
					}
					return nil
				}

				db.LogMessage(fmt.Sprintf("%d Medadaten werden verarbeitet", anzids), StateOk, true)
				workchannel := scheduler.Schedule(f, stringslicetoiface(processids))
			workloop:
				for {
					select {
					case workreply := <-workchannel:
						if err := workreply.Err; err != nil {
							logger.Panicln("Scheduler didn't return success:", err)
						} else if workreply.Code == schedule.StateFinish {
							tx.Commit()
							db.LogMessage("Idle", StateOk, true)
							break workloop
						}
					case <-time.After(time.Duration(heartbeatinterval) * time.Minute):
						logger.Println("Alive")
						db.HeartBeat()
					}
				}

			}
			select {
			case <-time.After(time.Duration(heartbeatinterval) * time.Minute):
				logger.Println("Alive")
				db.HeartBeat()
			}

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
