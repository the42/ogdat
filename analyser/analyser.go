package main

import (
	restful "github.com/emicklei/go-restful"
	"github.com/emicklei/go-restful/swagger"
	"github.com/garyburd/redigo/redis"
	"github.com/the42/ogdat/database"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

func (a analyser) listenredischannel(which string) chan []byte {
	a.rcom.Subscribe(which)
	retval := make(chan []byte)

	go func() {
		for {
			switch n := a.rcom.Receive().(type) {
			case redis.Message:
				println(n.Channel, n.Data)
				if n.Channel == which {
					retval <- n.Data
				}
			}
		}
	}()
	return retval
}

func (a analyser) populatedatasets() error {
	const (
		dskey   = "datasets"
		catkey  = "categories"
		verskey = "versions"
		entkey  = "entities"
		topokey = "toponyms"
	)

	logger.Println("SQL: Retrieving datasets")
	sets, err := a.dbcon.GetDatasets()
	if err != nil {
		return err
	}

	logger.Println("Deleting base dataset info keys from Redis")
	a.rcon.Do("DEL", catkey, verskey, entkey, topokey)
	a.rcon.DeleteKeyPattern(dskey+"*", "dataset:*")

	if err := a.rcon.Send("MULTI"); err != nil {
		return nil
	}

	logger.Println("Looping over datasets, populating information to Redis (this may take some time)")
	for _, set := range sets {

		// populate metadata version count
		if err = a.rcon.Send("ZINCRBY", verskey, 1, set.Version); err != nil {
			return err
		}
		// associate metadata version with ckanid
		if err = a.rcon.Send("SADD", dskey+":"+set.Version, set.CKANID); err != nil {
			return err
		}

		// populate entity count
		if err = a.rcon.Send("ZINCRBY", entkey, 1, set.Publisher); err != nil {
			return err
		}
		// associate entity with ckanid
		if err = a.rcon.Send("SADD", dskey+":"+set.Publisher, set.CKANID); err != nil {
			return err
		}

		// populate geographic toponym count
		if toponym := strings.TrimSpace(set.GeoToponym); len(toponym) > 0 {
			if err = a.rcon.Send("ZINCRBY", topokey, 1, toponym); err != nil {
				return err
			}
			// associate geographic toponym ckanid
			if err = a.rcon.Send("SADD", dskey+":"+toponym, set.CKANID); err != nil {
				return err
			}

		}

		// populate category count
		for _, cat := range set.Category {
			if err = a.rcon.Send("ZINCRBY", catkey, 1, cat); err != nil {
				return err
			}
			// associate category with ckanid
			if err = a.rcon.Send("SADD", dskey+":"+cat, set.CKANID); err != nil {
				return err
			}
		}

		// populate the dataset
		if err = a.rcon.Send("HMSET", redis.Args{}.Add("dataset:"+set.CKANID).AddFlat(&set)...); err != nil {
			return err
		}
	}
	logger.Println("Committing data to Redis")
	if _, err := a.rcon.Do("EXEC"); err != nil {
		return err
	}
	return nil
}

func (a analyser) populatedatasetbaseinfo() error {
	logger.Println("Starting populating datasets base info")
	if err := a.populatedatasets(); err != nil {
		return err
	}
	logger.Println("Done populating datasets base info")
	return nil
}

func (a analyser) populatean001() error {
	const an001 = "an001"

	logger.Println("AN001: What publishers have multiple metadata sets, but within distinct sets point to the same data")

	logger.Println("AN001: SQL: Retrieving data")
	sets, err := a.dbcon.GetAN001Data()
	if err != nil {
		return err
	}

	logger.Println("AN001: Deleting keys from Redis")
	a.rcon.DeleteKeyPattern(an001 + "*")

	if err := a.rcon.Send("MULTI"); err != nil {
		return nil
	}

	for _, set := range sets {

		if err = a.rcon.Send("ZINCRBY", an001+":"+set.CKANID, 1, set.Url); err != nil {
			return err
		}
	}
	logger.Println("AN001: Committing data to Redis")
	if _, err := a.rcon.Do("EXEC"); err != nil {
		return err
	}
	return nil
}

func (a analyser) populatean002() error {
	const an002 = "an002"

	logger.Println("AN002: What publishers have multiple metadata sets, but within distinct sets point to the same data")

	logger.Println("AN002: SQL: Retrieving data")
	sets, err := a.dbcon.GetAN002Data()
	if err != nil {
		return err
	}

	logger.Println("AN002: Deleting keys from Redis")
	a.rcon.DeleteKeyPattern(an002 + "*")

	if err := a.rcon.Send("MULTI"); err != nil {
		return nil
	}

	for _, set := range sets {

		if err = a.rcon.Send("ZINCRBY", an002+":"+set.CKANID, 1, set.Url); err != nil {
			return err
		}
	}
	logger.Println("AN002: Committing data to Redis")
	if _, err := a.rcon.Do("EXEC"); err != nil {
		return err
	}
	return nil
}

func (a analyser) populatebs001() error {
	// How many "last changed datasets" shall be retrieved?
	const num = 10

	const bs001 = "bs001"

	logger.Printf("BS001: Retrieve last %d changed datasets\n", num)

	logger.Println("BS001: SQL: Retrieving data")
	sets, err := a.dbcon.GetBS001Data(num)
	if err != nil {
		return err
	}

	logger.Println("BS001: Deleting keys from Redis")
	a.rcon.DeleteKeyPattern(bs001 + "*")

	if err := a.rcon.Send("MULTI"); err != nil {
		return nil
	}

	for _, set := range sets {
		if err = a.rcon.Send("SET", bs001+":"+set.CKANID, set.Time); err != nil {
			return err
		}
	}
	logger.Println("BS001: Committing data to Redis")
	if _, err := a.rcon.Do("EXEC"); err != nil {
		return err
	}
	return nil
}

func (a analyser) populatedatasetbaseanalysis() error {
	logger.Println("Starting dataset base analysis")

	if err := a.populatebs001(); err != nil {
		return err
	}

	logger.Println("Done dataset base analysis")
	return nil
}

func (a analyser) populatedatasetanalysis() error {
	logger.Println("Starting dataset analysis")

	if err := a.populatean001(); err != nil {
		return err
	}

	if err := a.populatean002(); err != nil {
		return err
	}

	logger.Println("Done dataset analysis")
	return nil
}

func (a analyser) populatedatasetinfo() error {
	if err := a.populatedatasetbaseinfo(); err != nil {
		return err
	}
	if err := a.populatedatasetbaseanalysis(); err != nil {
		return err
	}

	if err := a.populatedatasetanalysis(); err != nil {
		return err
	}
	return nil
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

	heartbeatinterval := getheartbeatinterval()
	heartbeatchannel := heartbeat(heartbeatinterval)

	<-heartbeatchannel // Wait for the first heartbeat, so the logging in the database is properly set up
	if err := analyser.populatedatasetinfo(); err != nil {
		logger.Panicln(err)
	}

	datachange := analyser.listenredischannel(watcherappid + ":DataChange")
	urlchange := analyser.listenredischannel(watcherappid + ":UrlChange")

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

func init() {
	logger = log.New(os.Stderr, filepath.Base(os.Args[0])+": ", log.LstdFlags)
}
