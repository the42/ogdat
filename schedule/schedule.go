package schedule

import (
	"sync"
)

type mapperfunc func([]interface{}) error

type State struct {
	Err  error
	Code int
}

const (
	StateTick int = iota + 1
	StateFinish
	StateError
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type schedule struct {
	workers int
}

func New(numworkers int) *schedule {
	if numworkers < 1 {
		panic("Schedule partition size must be greater than 0")
	}
	return &schedule{workers: numworkers}
}

func (s *schedule) GetWorkers() int {
	return s.workers
}

func (s *schedule) SetWorkers(numworkers int) {
	if numworkers < 1 {
		panic("Schedule partition size must be greater than 0")
	}
	s.workers = numworkers
}

func (s *schedule) Schedule(f mapperfunc, queue []interface{}) chan State {

	finish := make(chan State)
	go func() {
		var workslice []interface{}
		var wg sync.WaitGroup

		worklength := len(queue) / s.workers
		if len(queue) > 0 && worklength == 0 {
			worklength = len(queue)
		}
		for workerindex := 0; workerindex < s.workers; workerindex++ {

			workslice = queue[min(workerindex*worklength, len(queue)) : min((workerindex+1)*worklength, len(queue))]

			wg.Add(1)
			go func(ids []interface{}) {
				defer wg.Done()
				if err := f(ids); err != nil {
					finish <- State{Err: err, Code: StateError}
					return
				}
			}(workslice)
		}

		wg.Wait()
		finish <- State{Err: nil, Code: StateFinish}
	}()
	return finish
}
