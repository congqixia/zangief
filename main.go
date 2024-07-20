package main

import (
	"flag"
	"log"
	"time"

	"github.com/congqixia/zangief/milvus"
	"github.com/congqixia/zangief/presser"
	"go.uber.org/atomic"
)

var (
	addr     = flag.String("addr", "", "service instance")
	token    = flag.String("token", "", "service auth token")
	collName = flag.String("collection", "", "collection name to press")

	reqType = flag.String("reqType", "grpc", "service request type")

	workNum   = flag.Int("workNum", 100, "workerNum")
	interval  = flag.Duration("interval", time.Millisecond*100, "interval")
	tokens    = flag.Int("tokens", 10, "tokens per interval")
	totalTime = flag.Duration("totalTime", time.Minute*10, "total pressure time")
)

func main() {
	flag.Parse()

	w := milvus.NewMilvusSearchWorker(*addr, *token, *collName)
	durTotal := atomic.NewInt64(0)
	count := atomic.NewInt64(0)
	count.Inc()

	doPress(func() {
		var dur time.Duration
		switch *reqType {
		case "grpc":
			dur = w.SearchGrpc()
		case "restful":
			dur = w.SearchRestful()
		}
		durTotal.Add(int64(dur))
		count.Inc()
	}, *totalTime)

	log.Println("avg request time:", time.Duration(durTotal.Load()/count.Load()))
}

func doPress(w func(), dur time.Duration) {
	p := presser.NewPresser(*interval, *workNum, *tokens, w)

	p.Start()
	defer p.Stop()

	t := time.NewTimer(dur)

	<-t.C
}
