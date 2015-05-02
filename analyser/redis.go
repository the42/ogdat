package main

import (
	"fmt"
	"github.com/the42/ogdat/Godeps/_workspace/src/github.com/garyburd/redigo/redis"
)

const RedigoTimestamp = "2006-01-02 15:04:05.999999 -0700 MST"

func (a analyser) listenredischannel(which string) chan []byte {
	pubsubcon := redis.PubSubConn{a.pool.Get()}
	pubsubcon.Subscribe(which)
	retval := make(chan []byte)

	go func() {
		for {
			switch n := pubsubcon.Receive().(type) {
			case redis.Message:
				if n.Channel == which {
					retval <- n.Data
				}
			case error:
				fmt.Printf("Listining on redis channel %s failed: %v", which, n)
				return
			}
		}
	}()
	return retval
}
