package presser

import (
	"log"
	"sync"
	"time"
)

type Presser struct {
	wg       sync.WaitGroup
	workerWg sync.WaitGroup

	cOnce sync.Once
	close chan struct{}

	tokenCh chan struct{}
	work    func()

	intv      time.Duration
	workerNum int
	tokens    int
}

func NewPresser(intv time.Duration, workNum, tokens int, work func()) *Presser {
	return &Presser{
		intv:      intv,
		workerNum: workNum,
		tokens:    tokens,

		close:   make(chan struct{}),
		tokenCh: make(chan struct{}, tokens),

		work: work,
	}
}

func (p *Presser) Start() {

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

func (p *Presser) schedule() {
	ticker := time.NewTicker(p.intv)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		ADD_TOKEN:
			for i := 0; i < p.tokens; i++ {
				select {
				case p.tokenCh <- struct{}{}:
				default:
					log.Println("overpress detected")
					break ADD_TOKEN
				}
			}
		case <-p.close:
			return
		}
	}
}

func (p *Presser) worker() {
	for {
		select {
		case <-p.close:
			return
		case <-p.tokenCh:
			p.work()
		}
	}
}

func (p *Presser) Stop() {
	p.cOnce.Do(func() {
		close(p.close)
		p.wg.Wait()
		p.workerWg.Wait()
	})
}
