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
	rcon  database.RedisConn
	rcom  redis.PubSubConn
}

func NewAnalyser(dbcon *sql.DB, rcon redis.Conn) *analyser {
	analyser := &analyser{dbcon: analyserdb{DBConn: database.DBConn{Appid: AppID, DBer: dbcon}}, rcon: database.RedisConn{rcon}, rcom: redis.PubSubConn{rcon}}
	return analyser
}

func onlyweb() bool {
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

func getheartbeatinterval() int {

	if i, err := strconv.Atoi(os.Getenv("HEARTBEAT_INTERVAL")); err == nil {
		return i
	}
	return 60 // Minutes
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

func main() {
	rcon, err := database.GetRedisConnection(getredisconnect())
	if err != nil {
		logger.Panicln(err)
	}
	defer rcon.Close()

	dbcon, err := database.GetDatabaseConnection()
	if err != nil {
		logger.Panicln(err)
	}
	defer dbcon.Close()
	analyser := NewAnalyser(dbcon, rcon)

	var datachange, urlchange chan []byte
	var heartbeatchannel chan bool

	if !onlyweb() {
		heartbeatchannel = heartbeat(getheartbeatinterval())

		<-heartbeatchannel // Wait for the first heartbeat, so the logging in the database is properly set up
		if err := analyser.populatedatasetinfo(); err != nil {
			logger.Panicln(err)
		}
		datachange = analyser.listenredischannel(watcherappid + ":DataChange")
		urlchange = analyser.listenredischannel(watcherappid + ":UrlChange")
	}

	restful.DefaultResponseMimeType = restful.MIME_JSON
	restful.Add(NewAnalyseOGDATRESTService(analyser))

	config := swagger.Config{
		WebServicesUrl:  "http://localhost:8080",
		ApiPath:         "/apidoc",
		SwaggerPath:     "/doc/v1/",
		SwaggerFilePath: "swagger-ui/dist/",
		WebServices:     restful.RegisteredWebServices()} // you control what services are visible
	swagger.InstallSwaggerService(config)

	go logger.Fatal(http.ListenAndServe(":8080", nil))

	if !onlyweb() {
		for {
			select {
			case <-urlchange:
			case <-datachange:
				if err := analyser.populatedatasetinfo(); err != nil {
					logger.Panicln(err)
				}
			case <-heartbeatchannel:
				logger.Println("Idle")
			}
		}
	}
}

func init() {
	logger = log.New(os.Stderr, filepath.Base(os.Args[0])+": ", log.LstdFlags)
}
