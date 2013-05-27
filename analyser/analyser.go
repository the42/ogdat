package main

import (
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"github.com/the42/ogdat/database"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const AppID = "5bcbfc24-8e7e-4105-99c4-dd47e7e5094a"

var logger *log.Logger

type analyser struct {
	dbcon analyserdb
	rcon  redis.Conn
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

func (a analyser) populateunitsanz() error {
	const key = "entities"
	units, err := a.dbcon.GetUnitDSNums()
	if err != nil {
		return err
	}
	a.rcon.Do("DEL", key)
	for _, entanz := range units {
		if _, err = a.rcon.Do("ZADD", key, entanz.Numsets, entanz.Entity); err != nil {
			return err
		}
	}
	return nil
}

func (a analyser) populateversanz() error {
	const key = "versions"
	vers, err := a.dbcon.GetMDVersNums()
	if err != nil {
		return err
	}
	a.rcon.Do("DEL", key)
	for _, versanz := range vers {
		if _, err = a.rcon.Do("ZADD", key, versanz.Numsets, versanz.MetadataVersion); err != nil {
			return err
		}
	}
	return nil
}

func (a analyser) populatecategories() error {
	const key = "categories"
	cats, err := a.dbcon.GetCategories()
	if err != nil {
		return err
	}
	a.rcon.Do("DEL", key)

	for _, cat := range cats {
		var strcats []string
		if err := json.Unmarshal([]byte(cat), &strcats); err != nil {
			return err
		}
		for _, strcat := range strcats {
			if _, err = a.rcon.Do("ZINCRBY", key, 1, strcat); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a analyser) populatedatasetbaseinfo() error {

	if err := a.populateunitsanz(); err != nil {
		return err
	}
	if err := a.populateversanz(); err != nil {
		return err
	}
	if err := a.populatecategories(); err != nil {
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
	conn := analyserdb{DBConn: database.DBConn{Appid: AppID, DBer: dbcon}}
	analyser := &analyser{dbcon: conn, rcon: rcon}

	hertbeatinterval := getheartbeatinterval()
	heartbeatchannel := heartbeat(hertbeatinterval)

	if err = analyser.populatedatasetbaseinfo(); err != nil {
		logger.Panicln(err)
	}
	for {
		select {
		case <-heartbeatchannel:

		}
	}
}

func init() {
	logger = log.New(os.Stderr, filepath.Base(os.Args[0])+": ", log.LstdFlags)
}
