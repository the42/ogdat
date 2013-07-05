package main

import (
	"github.com/garyburd/redigo/redis"
)

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
