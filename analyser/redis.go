package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
)

func (a analyser) listenredischannel(which string) chan []byte {
	pubsubcon := redis.PubSubConn{a.pool.Get()}
	pubsubcon.Subscribe(which)
	retval := make(chan []byte)

	go func() {
		for {
			switch n := pubsubcon.Receive().(type) {
			case redis.Message:
				// TODO: remove after debugging
				println(n.Channel, n.Data)
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
