package main

import (
	"database/sql"
	restful "github.com/emicklei/go-restful"
	"github.com/emicklei/go-restful/swagger"
	"github.com/garyburd/redigo/redis"
	"github.com/the42/ogdat/database"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const AppID = "5bcbfc24-8e7e-4105-99c4-dd47e7e5094a"
const watcherappid = "a6545f8f-e0c9-4917-83c7-3e47bd1e0247"

var logger *log.Logger

type analyser struct {
	dbcon analyserdb
	pool  *redis.Pool
}

func NewAnalyser(dbcon *sql.DB, pool *redis.Pool) *analyser {
	analyser := &analyser{dbcon: analyserdb{DBConn: database.DBConn{Appid: AppID, DBer: dbcon}}, pool: pool}
	return analyser
}

func isonlyweb() bool {
	boolval, err := strconv.ParseBool(os.Getenv("ONLYWEB"))
	if err != nil {
		return false
	}
	return boolval
}

func getredisconnect() string {
	const redisurl = "REDISCLOUD_URL"
	const redisdb = "ANALYSER_REDISDB"

	return os.Getenv(redisurl) + "/" + os.Getenv(redisdb)
}

func portbinding() string {
	if port := os.Getenv("PORT"); port != "" {
		return port
	}
	return "8080"
}

func discoveryhost() string {
	if host := os.Getenv("DISCOVERYURL"); host != "" {
		return host
	}
	return "http://localhost"
}

func apibasepath() string {
	return os.Getenv("APIBASEPATH")
}

func getheartbeatinterval() int {

	if i, err := strconv.Atoi(os.Getenv("HEARTBEAT_INTERVAL")); err == nil {
		return i
	}
	return 60 // Minutes
}

func heartbeat(interval int) chan bool {
	retchan := make(chan bool)
	go func() {
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
	}()
	return retchan
}

func main() {
	dbcon, err := database.GetDatabaseConnection()
	if err != nil {
		logger.Panicln(err)
	}
	defer dbcon.Close()
	analyser := NewAnalyser(dbcon, redis.NewPool(func() (redis.Conn, error) { return database.GetRedisConnection(getredisconnect()) }, 10))

	var datachange, urlchange chan []byte
	var heartbeatchannel chan bool

	populatedatasetinfo := func() {
		if err := analyser.populatedatasetinfo(); err != nil {
			logger.Panicln(err)
		}
	}

	if !isonlyweb() {
		heartbeatchannel = heartbeat(getheartbeatinterval())
		<-heartbeatchannel // Wait for the first heartbeat, so the logging in the database is properly set up

		populatedatasetinfo()

		datachange = analyser.listenredischannel(watcherappid + ":DataChange")
		urlchange = analyser.listenredischannel(watcherappid + ":UrlChange")
	}

	restful.DefaultResponseMimeType = restful.MIME_JSON
	restful.EnableContentEncoding = true
	restful.Add(NewAnalyseOGDATRESTService(analyser))

	config := swagger.Config{
		WebServicesUrl:  discoveryhost(),
		ApiPath:         "/swaggerdoc",
		SwaggerPath:     "/doc/",
		SwaggerFilePath: "swagger-ui/dist/",
		WebServices:     restful.RegisteredWebServices()} // you control what services are visible
	swagger.InstallSwaggerService(config)

	logger.Printf("analyser (%s) listening on port %s\n", AppID, portbinding())
	go func() {
		logger.Fatal(http.ListenAndServe(":"+portbinding(), nil))
	}()

	if !isonlyweb() {
		for {
			select {
			case <-urlchange:
				// TODO:naive approach here. If a URLChange or DataChange event is triggered,
				// the whole analytic database will be recreated. It would be better to trace
				// only the affected datasets and only create the relevant statistic.
				// In future, urlchange/datachange might contain a JSON-encoded []byte which contains
				// the affected IDs
				logger.Println("Received URL change notice, re-generating database analysis")
				populatedatasetinfo()
			case <-datachange:
				logger.Println("Received Data change notice, re-generating database analysis")
				populatedatasetinfo()
			case <-heartbeatchannel:
				logger.Println("Idle")
			}
		}
	}
}

func init() {
	logger = log.New(os.Stderr, filepath.Base(os.Args[0])+": ", log.LstdFlags)
}
