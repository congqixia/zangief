package stat

import (
	"log"
	"sort"
	"sync"
	"time"
)

// Epoch maintains request info of certain period of time.
type Epoch struct {
	sync.Mutex
	requestTime []time.Duration
	startTime   time.Time
	total       int64
	count       int64

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

func (e *Epoch) Stat() {
	// wait all worker done
	e.wg.Wait()
	if e.count == 0 {
		log.Println("no record this epoch")
		return
	}
	sort.Slice(e.requestTime, func(i, j int) bool {
		return e.requestTime[i] < e.requestTime[j]
	})
	num := len(e.requestTime)
	p99Idx := num * 99 / 100
	log.Printf("Epoch %s, total requests: %d, avg: %v, p99: %v\n", e.startTime.Format("2006-01-02 15:04:06"), num, time.Duration(e.total/e.count), e.requestTime[p99Idx])
}

func NewEpoch(startTime time.Time, estSize int) *Epoch {
	return &Epoch{
		requestTime: make([]time.Duration, 0, estSize),
		startTime:   startTime,
	}
}
