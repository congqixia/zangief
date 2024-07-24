package stat

import (
	"log"
	"sort"
	"sync"
	"time"

	"go.uber.org/atomic"
)

// Epoch maintains request info of certain period of time.
type Epoch struct {
	sync.Mutex
	requestTime []time.Duration
	startTime   time.Time
	total       int64
	count       int64
	estSize     int64
	idx         int

	errorCount atomic.Int32

	wg sync.WaitGroup
}

func (e *Epoch) Add() {
	e.wg.Add(1)
}

func (e *Epoch) Done() {
	e.wg.Done()
}

func (e *Epoch) Record(rt time.Duration) {
	e.Lock()
	defer e.Unlock()
	// TODO use heap if request number is determined
	e.requestTime = append(e.requestTime, rt)
	e.total += int64(rt)
	e.count++
}

// RecordError records an failure request with error.
func (e *Epoch) RecordError(err error) {
	e.errorCount.Inc()
}

// Stat prints statistics for this epoch.
func (e *Epoch) Stat() {
	// wait all worker done
	e.wg.Wait()
	if e.count == 0 {
		log.Println("no record this epoch")
		return
	}

	num := len(e.requestTime)
	log.Printf("Epoch %d, Expected request: %d,  Actual requests, success: %d, failure: %d, Overloaded: %t\n", e.idx, e.estSize, num, e.errorCount.Load(), len(e.requestTime)+int(e.errorCount.Load()) < int(e.estSize))

	sort.Slice(e.requestTime, func(i, j int) bool {
		return e.requestTime[i] < e.requestTime[j]
	})

	p99Idx := num * 99 / 100
	log.Printf("Start time: %s, Avg: %v, p99: %v\n", e.startTime.Format("2006-01-02 15:04:06"), time.Duration(e.total/e.count), e.requestTime[p99Idx])
}

func NewEpoch(idx int, startTime time.Time, estSize int) *Epoch {
	return &Epoch{
		requestTime: make([]time.Duration, 0, estSize),
		startTime:   startTime,
		idx:         idx,
		estSize:     int64(estSize),
	}
}
