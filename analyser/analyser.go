package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/the42/ogdat/database"
	"log"
	"os"
	"path/filepath"
)

const AppID = "5bcbfc24-8e7e-4105-99c4-dd47e7e5094a"

var logger *log.Logger

func getredisconnect() string {
	const redisurl = "REDISCLOUD_URL"
	const redisdb = "ANALYSER_REDISDB"

	return os.Getenv(redisurl) + "/" + os.Getenv(redisdb)
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

	// BEGIN
	// some random tests
	rcon.Do("SET", "foo", 1)
	exists, _ := redis.Bool(rcon.Do("EXISTS", "foo"))
	fmt.Printf("%#v\n", exists)
	rcon.Do("DEL", "foo")
	exists, _ = redis.Bool(rcon.Do("EXISTS", "foo"))
	fmt.Printf("%#v\n", exists)
	// END

}

func init() {
	logger = log.New(os.Stderr, filepath.Base(os.Args[0])+": ", log.LstdFlags)
}
