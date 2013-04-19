package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/the42/ogdat"
	"github.com/the42/ogdat/ckan"
	"github.com/the42/ogdat/ogdatv21"
	"github.com/the42/ogdat/schedule"
	"log"
	"os"
	"os/signal"
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
var servetdb = flag.Bool("serve", false, "Start in watchdog mode. Process will continue to run until it receives a (clean shutdown) or gets killed")

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
	return 60 // Minutes
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

func heartbeat(interval int) {
	for {
		dbconn := GetDatabaseConnection()
		db := &DBConn{dbconn, AppID}
		if err := db.HeartBeat(); err != nil {
			logger.Panicln(err)
		}
		dbconn.Close()
		logger.Println("Watchdog alive")
		time.Sleep(time.Duration(interval) * time.Minute)
	}
}

func dataurlslicetoiface(dus []DataUrl) []interface{} {
	slice := make([]interface{}, len(dus))
	for i, v := range dus {
		slice[i] = v
	}
	return slice
}

func ifaceslicetodataurl(ifs []interface{}) []DataUrl {
	slice := make([]DataUrl, len(ifs))
	for i, v := range ifs {
		s, ok := v.(DataUrl)
		if !ok {
			panic("Interface value not of DataUrl type")
		}
		slice[i] = s
	}
	return slice
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

	nums := len(processids)
	for idx, id := range processids {

		logger.Printf("%4d / %4d : processing %v\n", idx+1, nums, id)

		mdjson, err := portal.GetDatasetStreamforID(id, true)
		if err != nil {
			return fmt.Errorf("Cannot fetch JSON for ID %v: %s", id, err)
		}

		md, err := ogdatv21.MetadatafromJSONStream(mdjson)
		if err != nil {
			return fmt.Errorf("Cannot access metadata for ID %v: %s", id, err)
		}

		dbdatasetid, isnew, err := conn.InsertOrUpdateMetadataInfo(id, md)
		if err != nil {
			return fmt.Errorf("InsertOrUpdateMetadataInfo: database error at id %v: %s", id, err)
		}

		messages, err := md.Check(true)
		if err != nil {
			return fmt.Errorf("Metadata check error for id %v: %s", id, err)
		}

		if err = conn.ProtocollCheck(dbdatasetid, isnew, messages); err != nil {
			return fmt.Errorf("ProtocollCheck: database error at id %v: %s", id, err)
		}
	}
	logger.Printf("Worker finished processing %d entries", nums)
	return nil
}

func processdataseturls(conn *DBConn, urls []DataUrl) error {

	nums := len(urls)
	message := make([]ogdat.CheckMessage, 1)
	for idx, url := range urls {

		logger.Printf("%4d / %4d : processing %v\n", idx+1, nums, url.Url)

		_, checkresult := ogdat.FetchHead(url.Url)

		message[0].Type = checkresult.Status
		message[0].Text = url.Url
		message[0].OGDID = url.Field_id
		if err := conn.ProtocollCheck(url.DatasetID, true, message); err != nil {
			return fmt.Errorf("ProtocollCheck: database error at id %v: %s", url.DatasetID, err)
		}
	}
	logger.Printf("Worker finished processing %d entries", nums)
	return nil
}

func checkdata(dbconnection *sql.DB) error {

	hit, err := db.GetLastHit()
	if err != nil {
		return fmt.Errorf("Cannot read last DBHit: %s", err)
	}

	var processids []string
	if hit == nil {
		logger.Println("No checkpoint in database found, getting all datasets")
		processids, err = portal.GetAllMetaDataIDs()
	} else {
		logger.Printf("Getting changed datasets since %s\n", hit)
		processids, err = portal.GetChangedPackageIDsSince(*hit, getnumworkers())
	}

	if err != nil {
		return err
	}

	if anzids := len(processids); anzids > 0 {

		tx, err := dbconnection.Begin()
		if err != nil {
			return fmt.Errorf("Cannot create database transaction: %s", err)
		}
		scheduler := schedule.New(getnumworkers())
		logger.Printf("Doing %d jobs in parallel\n", scheduler.GetWorkers())
		conn := &DBConn{DBer: tx, appid: AppID}
		f := func(slice []interface{}) error {
			if err := processmetadataids(conn, ifaceslicetostring(slice)); err != nil {
				return err
			}
			return nil
		}

		db.LogMessage(fmt.Sprintf("%d Medadaten werden verarbeitet", anzids), StateOk, true)
		workchannel := scheduler.Schedule(f, stringslicetoiface(processids))
		select {
		case workreply := <-workchannel:
			if err := workreply.Err; err != nil {
				return fmt.Errorf("Scheduler didn't return success: %s", err)
			} else if workreply.Code == schedule.StateFinish {
				tx.Commit()
				db.LogMessage("Idle", StateOk, true)
				logger.Printf("Finished processing %d datasets\n", anzids)
			}
		}

	}
	return nil
}

func checkurls(dbconnection *sql.DB) error {

	urls, err := db.GetDataUrls()
	if err != nil {
		return err
	}

	if anzurls := len(urls); anzurls > 0 {

		tx, err := dbconnection.Begin()
		if err != nil {
			return fmt.Errorf("Cannot create database transaction: %s", err)
		}

		scheduler := schedule.New(getnumworkers())
		logger.Printf("Doing %d jobs in parallel\n", scheduler.GetWorkers())

		conn := &DBConn{DBer: tx, appid: AppID}

		f := func(slice []interface{}) error {
			if err := processdataseturls(conn, ifaceslicetodataurl(slice)); err != nil {
				return err
			}
			return nil
		}

		db.LogMessage(fmt.Sprintf("%d Urls werden gecheckt", anzurls), StateOk, true)
		workchannel := scheduler.Schedule(f, dataurlslicetoiface(urls))

		select {
		case workreply := <-workchannel:
			if err := workreply.Err; err != nil {
				return fmt.Errorf("Scheduler didn't return success: %s", err)
			} else if workreply.Code == schedule.StateFinish {
				tx.Commit()
				db.LogMessage("Idle", StateOk, true)
				logger.Printf("Finished checking %d Urls\n", anzurls)
			}
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

	// When the process gets killed, try to delete the lock file
	interrupt := make(chan os.Signal)
	signal.Notify(interrupt, os.Interrupt)
	go func() {
		<-interrupt
		logger.Println("Terminate requested")
		lockfile.Delete()
		os.Exit(10)
	}()

	dbconnection := GetDatabaseConnection()
	db = &DBConn{dbconnection, AppID}
	defer dbconnection.Close()

	if *resettdb {
		resetdb()
		logger.Println("Info: Earyl exit due to maintainance switches")
		return 2
	}

	if *servetdb {

		portal = ckan.NewDataPortalAPIEndpoint(getckanurl(), "2/")
		heartbeatinterval := getheartbeatinterval()
		go heartbeat(heartbeatinterval)

		urlcheckpointchan := time.Tick(1 * time.Hour * 4)
		datacheckpointchan := time.Tick(1 * time.Hour * 4)

		if err := checkdata(dbconnection); err != nil {
			logger.Panicln(err)
		}

		for {

			select {
			case <-urlcheckpointchan:
				if err := checkurls(dbconnection); err != nil {
					logger.Panicln(err)
				}
			case <-datacheckpointchan:
				if err := checkdata(dbconnection); err != nil {
					logger.Panicln(err)
				}
			default:
				logger.Printf("Nothing to do, sleeping for %d minutes\n", heartbeatinterval)
				time.Sleep(time.Duration(heartbeatinterval) * time.Minute)
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
