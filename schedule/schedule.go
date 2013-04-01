package schedule

import (
	"sync"
)

type mapperfunc func([]interface{})

type State int

const (
	StateTick State = iota + 1
	StateFinish
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Schedule(queue []interface{}, partsize int, f mapperfunc) chan State {
	if partsize < 1 {
		panic("Schedule partition size must be greater than 0")
	}

	finish := make(chan State)
	go func() {
		var workslice []interface{}
		var wg sync.WaitGroup

		worklength := len(queue) / partsize
		for workerindex := 0; workerindex < partsize; workerindex++ {

			workslice = queue[workerindex*worklength : min((workerindex+1)*worklength, len(queue))]

			wg.Add(1)
			go func(ids []interface{}) {
				defer wg.Done()
				f(ids)
			}(workslice)
		}

		wg.Wait()
		finish <- StateFinish
	}()
	return finish
}
