package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

var logger *log.Logger

func getredisconnect() string {
	const redisurl = "REDISCLOUD_URL"
	const redisdb = "ANALYSER_REDISDB"

	return os.Getenv(redisurl) + "/" + os.Getenv(redisdb)
}

// taken from https://github.com/soveran/redisurl/blob/master/redisurl.go
func connectToURL(s string) (c redis.Conn, err error) {
	redisURL, err := url.Parse(s)

	if err != nil {
		return
	}

	auth := ""

	if redisURL.User != nil {
		if password, ok := redisURL.User.Password(); ok {
			auth = password
		}
	}

	c, err = redis.Dial("tcp", redisURL.Host)

	if err != nil {
		fmt.Println(err)
		return
	}

	if len(auth) > 0 {
		_, err = c.Do("AUTH", auth)

		if err != nil {
			fmt.Println(err)
			return
		}
	}

	if dbs := strings.Split(redisURL.Path, "/"); len(dbs) > 1 && dbs[1] != "" {
		c.Do("SELECT", dbs[1])
	}

	return
}

func main() {
	rcon, err := connectToURL(getredisconnect())
	if err != nil {
		logger.Panicln(err)
	}
	defer rcon.Close()
	rcon.Do("SET", "foo", 1)
	exists, _ := redis.Bool(rcon.Do("EXISTS", "foo"))
	fmt.Printf("%#v\n", exists)
	rcon.Do("DEL", "foo")
	exists, _ = redis.Bool(rcon.Do("EXISTS", "foo"))
	fmt.Printf("%#v\n", exists)
	
}

func init() {
	logger = log.New(os.Stderr, filepath.Base(os.Args[0])+": ", log.LstdFlags)
}
