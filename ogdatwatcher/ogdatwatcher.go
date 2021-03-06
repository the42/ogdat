package main

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/the42/ogdat"
	"github.com/the42/ogdat/ckan"
	"github.com/the42/ogdat/database"
	"github.com/the42/ogdat/ogdatv21"
	"github.com/the42/ogdat/ogdatv22"
	"github.com/the42/ogdat/ogdatv23"
	"github.com/the42/ogdat/schedule"
)

const AppID = "a6545f8f-e0c9-4917-83c7-3e47bd1e0247"

var logger *log.Logger
var watcherdatabase *watcherdb
var portal *ckan.Portal

var resettdb = flag.Bool("resetdb", false, "Delete the tracking database. You will be prompted before actual deletion. Process will terminate afterwards.")
var servetdb = flag.Bool("serve", false, "Start in watchdog mode. Process will continue to run until it receives a (clean shutdown) or gets killed")
var sdidle = flag.Duration("sdidle", -1, "Shutdown the process when the next action is longer than x minutes ahead")

func gotyesonprompt() bool {
	var prompt string
	fmt.Scanf("%s", &prompt)
	prompt = strings.ToLower(strings.TrimSpace(prompt))
	if len(prompt) > 0 {
		return prompt[0] == 'y'
	}
	return false
}

func gettimezone() string {
	return os.Getenv("TZ")
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

func getredisconnect() string {
	const redisurl = "REDISCLOUD_URL"

	return os.Getenv(redisurl)
}

func redispublishint(key string, num int) error {
	c, err := database.GetRedisConnection(getredisconnect())
	if err != nil {
		rcon := database.RedisConn{Conn: c}
		rcon.Publish(AppID+":"+key, fmt.Sprintf("%d", num))
		rcon.Flush()
		rcon.Close()
	}
	return err
}

func resetdb() {
	logger.Println("Warning: Requesting database reset")
	fmt.Print("\n\nALL RECORDED DATA IN DATABASE  WILL BE DELETED.\nDO YOU REALLY WANT TO PROCEED? [N,y]\n")
	if !gotyesonprompt() {
		fmt.Print("\nABORTING\n\n")
		logger.Println("Info: Database reset canceled")
	} else {
		if err := watcherdatabase.ResetDatabase(); err != nil {
			s := fmt.Sprintf("Database reset failed: %s", err)
			fmt.Println(s)
			logger.Panic(s)
		}
	}
}

func heartbeat(interval int) chan bool {
	retchan := make(chan bool)
	f := func() {
		for {
			dbconn, err := database.GetDatabaseConnection()
			if err != nil {
				logger.Panicln(err)
			}
			db := &database.DBConn{DBer: dbconn, Appid: AppID}
			if err := db.HeartBeat(); err != nil {
				logger.Panicln(err)
			}
			dbconn.Close()
			logger.Printf("Watchdog beating every %d minute\n", interval)
			retchan <- true
			time.Sleep(time.Duration(interval) * time.Minute)
		}
	}
	go f()
	return retchan
}

func dataurlslicetoiface(dus [][]DataUrl) []interface{} {
	slice := make([]interface{}, len(dus))
	for i, v := range dus {
		slice[i] = v
	}
	return slice
}

func ifaceslicetodataurl(ifs []interface{}) [][]DataUrl {
	slice := make([][]DataUrl, len(ifs))
	for i, v := range ifs {
		s, ok := v.([]DataUrl)
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

func processmetadataids(conn *watcherdb, processids []string) error {

	nums := len(processids)
	var md ogdat.Metadater
	var jsonparseerror error
	var messages []ogdat.CheckMessage

nextid:
	for idx, id := range processids {

		md = nil
		jsonparseerror = nil
		messages = nil

		logger.Printf("%4d / %4d : processing %v\n", idx+1, nums, id)

		mdjsonreader, err := portal.GetDatasetStreamforID(id, true)
		// if the dataset could not be found, mark it as deleted
		switch portalerror := err.(type) {
		case ckan.PortalError:
			switch portalerror.StatusCode {
			// If a dataset was once available but has been deleted, the server will return with access denied;
			// if it is not available at all, we may also assume it is deleted
			case ckan.StatusForbidden:
				_, err := conn.MarkDatasetDeleted(id)
				if err != nil {
					return fmt.Errorf("Cannot mark dataset with ckanid %s as deleted: %s", id, err)
				}
				break nextid
			}
		}

		buf, _ := ioutil.ReadAll(mdjsonreader)
		minimaljsonbuffer := bytes.NewBuffer(buf)
		mdjson := bytes.NewBuffer(buf)

		if err != nil {
			return fmt.Errorf("Cannot fetch JSON for ID %v: %s", id, err)
		}

		mmd, err := ogdat.MinimalMetaDataforJSONStream(minimaljsonbuffer)
		if err != nil {
			return fmt.Errorf("Cannot access minimal metadata for ID %v: %s", id, err)
		}
		if mmd == nil {
			logger.Printf("Info: Minimal Metadata for ID %v could not be parsed, error returned?\n", id)
			continue
		}
		var version string
		if mmd.Schema_Name != nil {
			version = ogdat.OGDVersionfromString(*mmd.Schema_Name)
		}

		dbdatasetid, isnew, err := conn.InsertOrUpdateMetadataInfo(id, mmd)
		if err != nil {
			return fmt.Errorf("InsertOrUpdateMetadataInfo: database error at id %v: %s", id, err)
		}

		switch version {
		case "2.0", "2.1":
			md, jsonparseerror = ogdatv21.MetadatafromJSONStream(mdjson)
		case "2.2":
			md, jsonparseerror = ogdatv22.MetadatafromJSONStream(mdjson)
		case "2.3":
			md, jsonparseerror = ogdatv23.MetadatafromJSONStream(mdjson)
		case "":
			logger.Printf("No Metadata Schema given for ID %v, skipping", id)
			messages = []ogdat.CheckMessage{{Type: ogdat.Info, Text: "Kein Schema spezifiziert, Metadaten können nicht überprüft werden", OGDID: -1}}
		default:
			logger.Printf("Identified Metadata Version %s but no checker implemented", version)
			s := fmt.Sprintf("Für die Metadatenversion %s ist keine Überprüfung implementiert", version)
			messages = []ogdat.CheckMessage{{Type: ogdat.Info, Text: s, OGDID: -1}}
		}

		if jsonparseerror != nil {
			return fmt.Errorf("Cannot parse metadata for ID %v: %s", id, jsonparseerror)
		}
		if messages == nil {
			messages, err = md.Check(true)
			if err != nil {
				return fmt.Errorf("Metadata check error for id %v: %s", id, err)
			}
		}

		if err = conn.ProtocollCheck(dbdatasetid, isnew, messages); err != nil {
			return fmt.Errorf("ProtocollCheck: database error at id %v: %s", id, err)
		}
	}
	logger.Printf("Worker finished processing %d entries\n", nums)
	return nil
}

func processdataseturls(conn *watcherdb, nestedurls [][]DataUrl) error {

	var anz int
	for setidx, urls := range nestedurls {
		logger.Printf("%4d / %4d", setidx+1, len(nestedurls))
		messages := make([]ogdat.CheckMessage, len(urls))
		for idx, url := range urls {
			logger.Printf("%4d / %4d: processing %s", idx+1, len(urls), url.Url)

			_, checkresult := ogdat.FetchHead(url.Url)

			messages[idx].Type = checkresult.Status
			messages[idx].Text = checkresult.Context
			messages[idx].OGDID = url.Field_id

			anz++
		}
		if err := conn.ProtocollCheck(urls[0].DatasetID, true, messages); err != nil {
			return fmt.Errorf("ProtocollCheck: database error at id %v: %s", urls[setidx].DatasetID, err)
		}
	}
	logger.Printf("Worker finished processing %d entries\n", anz)
	return nil
}

func checkdata(dbconnection *sql.DB) (int, error) {

	hit, err := watcherdatabase.GetLastHit()
	if err != nil {
		return 0, fmt.Errorf("Cannot read last DBHit: %s", err)
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
		return 0, err
	}

	anzids := len(processids)
	if anzids > 0 {

		tx, err := dbconnection.Begin()
		if err != nil {
			return 0, fmt.Errorf("Cannot create database transaction: %s", err)
		}
		scheduler := schedule.New(getnumworkers())
		logger.Printf("Doing %d jobs in parallel\n", scheduler.GetWorkers())
		conn := &watcherdb{database.DBConn{DBer: tx, Appid: AppID}}
		f := func(slice []interface{}) error {
			if err := processmetadataids(conn, ifaceslicetostring(slice)); err != nil {
				return err
			}
			return nil
		}

		watcherdatabase.LogMessage(fmt.Sprintf("%d Medadaten werden verarbeitet", anzids), database.StateOk, true)
		workchannel := scheduler.Schedule(f, stringslicetoiface(processids))
		select {
		case workreply := <-workchannel:
			if err := workreply.Err; err != nil {
				return 0, fmt.Errorf("Scheduler didn't return success: %s", err)
			} else if workreply.Code == schedule.StateFinish {
				tx.Commit()
				watcherdatabase.LogMessage("Idle", database.StateOk, true)
				logger.Printf("Finished processing %d datasets\n", anzids)
			}
		}
	}
	return anzids, nil
}

func checkurls(dbconnection *sql.DB) (int, error) {

	urls, err := watcherdatabase.GetDataUrls()
	if err != nil {
		return 0, err
	}

	anzurls := len(urls)
	if anzurls > 0 {

		tx, err := dbconnection.Begin()
		if err != nil {
			return 0, fmt.Errorf("Cannot create database transaction: %s", err)
		}

		scheduler := schedule.New(getnumworkers())
		logger.Printf("Doing %d jobs in parallel\n", scheduler.GetWorkers())

		conn := &watcherdb{database.DBConn{DBer: tx, Appid: AppID}}

		f := func(slice []interface{}) error {
			if err := processdataseturls(conn, ifaceslicetodataurl(slice)); err != nil {
				return err
			}
			return nil
		}

		watcherdatabase.LogMessage(fmt.Sprintf("%d Urls werden gecheckt", anzurls), database.StateOk, true)
		workchannel := scheduler.Schedule(f, dataurlslicetoiface(urls))

		select {
		case workreply := <-workchannel:
			if err := workreply.Err; err != nil {
				return 0, fmt.Errorf("Scheduler didn't return success: %s", err)
			} else if workreply.Code == schedule.StateFinish {
				tx.Commit()
				watcherdatabase.LogMessage("Idle", database.StateOk, true)
				logger.Printf("Finished checking %d Urls\n", anzurls)
			}
		}
	}
	return anzurls, nil
}

func date(t time.Time, loc *time.Location) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, loc)
}

func nextWeekday(t time.Time, w time.Weekday, d time.Duration, loc *time.Location) time.Time {
	s := date(t.Add(-d), loc)
	u := int(w - s.Weekday())
	if u <= 0 {
		u += 7
	}
	s = s.AddDate(0, 0, u)
	s = s.Add(d)
	return s
}

func nextHour(t time.Time, d time.Duration, loc *time.Location) time.Time {
	s := date(t.Add(-d), loc)
	s = s.Add(d + 24*time.Hour)
	return s
}

// An Url check happens every Sunday at 21 o'clock
func urlchecktime(loc *time.Location) time.Time {
	t := time.Now().In(loc)
	return nextWeekday(t, time.Sunday, 21*time.Hour, loc)
}

// A data check happens every day at 23 o'clock
func datachecktime(loc *time.Location) time.Time {
	t := time.Now().In(loc)
	return nextHour(t, 23*time.Hour, loc)
}

func mymain() int {

	if flag.NFlag() == 0 {
		fmt.Println("No command line flags given. Usage:")
		flag.PrintDefaults()
		logger.Panicln("Fatal: No command line flags given")
	}

	dbconnection, err := database.GetDatabaseConnection()
	if err != nil {
		logger.Panicln(err)
	}

	watcherdatabase = &watcherdb{DBConn: database.DBConn{DBer: dbconnection, Appid: AppID}}
	defer dbconnection.Close()

	if *resettdb {
		resetdb()
		logger.Println("Info: Earyl exit due to maintainance switches")
		return 1
	}

	if *servetdb {

		portal = ckan.NewDataPortalAPIEndpoint(getckanurl(), "2/")
		heartbeatinterval := getheartbeatinterval()
		heartbeatchannel := heartbeat(heartbeatinterval)

		loc, err := time.LoadLocation(gettimezone())
		if err != nil {
			logger.Panicln(err)
		}

		logger.Printf("Processing relative to timezone %s\n", loc)

		whenurlcheck := urlchecktime(loc)
		whendatacheck := datachecktime(loc)

		datacheckchan := time.After(0) // assign a ticker of 0 to immediately trigger a data check
		urlcheckchan := time.After(whenurlcheck.Sub(time.Now().In(loc)))

		for {
			select {
			case <-urlcheckchan:
				anz, err := checkurls(dbconnection)
				if err != nil {
					logger.Panicln(err)
				}
				if anz > 0 {
					if err := redispublishint("UrlChange", anz); err != nil {
						logger.Printf("Cannot publish url change to redis: %s\n", err)
					}
				}
				whenurlcheck = urlchecktime(loc)
				urlcheckchan = time.After(whenurlcheck.Sub(time.Now().In(loc)))
			case <-datacheckchan:
				anz, err := checkdata(dbconnection)
				if err != nil {
					logger.Panicln(err)
				}
				if anz > 0 {
					if err := redispublishint("DataChange", anz); err != nil {
						logger.Printf("Cannot publish data change to redis: %s\n", err)
					}
				}
				whendatacheck = datachecktime(loc)
				datacheckchan = time.After(whendatacheck.Sub(time.Now().In(loc)))
			case <-time.After(time.Duration(heartbeatinterval) * time.Minute):
			}

			now := time.Now().In(loc)
			logger.Printf("%v: Nothing to do\n", now)

			datacheckdiff := whendatacheck.Sub(now)
			urlcheckdiff := whenurlcheck.Sub(now)

			logger.Printf("Next Data check in %v\n", datacheckdiff)
			logger.Printf("Next Url check in %v\n", urlcheckdiff)

			// drain the heartbeat channel; without draining, the heartbeat won't get written to the database
			select {
			case <-heartbeatchannel:
			}

			if sdidle != nil && *sdidle > 0 {
				if datacheckdiff > *sdidle && urlcheckdiff > *sdidle {
					logger.Printf("Next activity is more than %v ahead, terminating\n", *sdidle)
					return 0
				}
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
