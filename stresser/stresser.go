package stresser

import (
	"sync"
	"time"

	"github.com/congqixia/zangief/stat"
)

type Stresser struct {
	wg       sync.WaitGroup
	workerWg sync.WaitGroup

	cOnce sync.Once
	close chan struct{}

	tokenCh chan *stat.Epoch
	work    func(*stat.Epoch)

	ppe       int // number of interval per epoch
	intv      time.Duration
	workerNum int
	tokens    int
}

func NewStresser(intv time.Duration, workNum, tokens int, periodPerEpoch int, work func(*stat.Epoch)) *Stresser {
	return &Stresser{
		intv:      intv,
		workerNum: workNum,
		tokens:    tokens,

		close:   make(chan struct{}),
		tokenCh: make(chan *stat.Epoch, tokens),

		work: work,
		ppe:  300,
	}
}

func (p *Stresser) Start() {

	p.workerWg.Add(p.workerNum)
	for i := 0; i < p.workerNum; i++ {
		go func() {
			defer p.workerWg.Done()
			p.worker()
		}()
	}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.schedule()
	}()
}

func (p *Stresser) schedule() {
	ticker := time.NewTicker(p.intv)
	defer ticker.Stop()
	var currentEpoch *stat.Epoch
	var idx int
	var eidx int
	for {
		select {
		case t := <-ticker.C:
			if currentEpoch == nil {
				currentEpoch = stat.NewEpoch(idx, t, p.tokens*p.ppe)
				idx++
			}
		ADD_TOKEN:
			for i := 0; i < p.tokens; i++ {
				select {
				case p.tokenCh <- currentEpoch:
					currentEpoch.Add()
				default:
					break ADD_TOKEN
				}
			}
			eidx++
			if eidx%p.ppe == 0 {
				go currentEpoch.Stat()
				currentEpoch = nil
			}
		case <-p.close:
			// print last epoch statistics
			if currentEpoch != nil {
				currentEpoch.Stat()
			}
			return
		}
	}
}

func (p *Stresser) worker() {
	for {
		select {
		case <-p.close:
			return
		case e := <-p.tokenCh:
			p.work(e)
		}
	}
}

func (p *Stresser) Stop() {
	p.cOnce.Do(func() {
		close(p.close)
		p.wg.Wait()
		p.workerWg.Wait()
	})
}
