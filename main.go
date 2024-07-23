package main

import (
	"flag"
	"time"

	"github.com/congqixia/zangief/milvus"
	"github.com/congqixia/zangief/presser"
	"github.com/congqixia/zangief/stat"
)

var (
	addr     = flag.String("addr", "", "service instance")
	token    = flag.String("token", "", "service auth token")
	collName = flag.String("collection", "", "collection name to press")

	reqType = flag.String("reqType", "grpc", "service request type")
	topK    = flag.Int("topk", 1000, "search topK value, default 1000")

	workNum        = flag.Int("workNum", 100, "workerNum, default 100")
	interval       = flag.Duration("interval", time.Millisecond*100, "interval")
	tokens         = flag.Int("tokens", 10, "tokens per interval")
	periodPerEpoch = flag.Int("periodPerEpoch", 300, "period number per epoch, default 300 (300*interval per epoch, default 30s)")
	totalTime      = flag.Duration("totalTime", time.Minute*10, "total pressure time, default 10min")

	restfulPath = flag.String("restfulPath", "", "restful url path")
)

func main() {
	flag.Parse()

	opts := []milvus.MilvusOption{}
	if *restfulPath != "" {
		opts = append(opts, milvus.WithRestfulPath(*restfulPath))
	}

	w := milvus.NewMilvusSearchWorker(*addr, *token, *collName, opts...)

	doPress(func(e *stat.Epoch) {
		defer e.Done()
		var dur time.Duration
		var err error
		switch *reqType {
		case "grpc":
			dur, err = w.SearchGrpc(*topK)
		case "restful":
			dur, err = w.SearchRestful(*topK)
		}
		if err != nil {
			return
		}
		e.Record(dur)
	}, *totalTime)
}

func doPress(w func(*stat.Epoch), dur time.Duration) {
	p := presser.NewPresser(*interval, *workNum, *tokens, *periodPerEpoch, w)

	p.Start()
	defer p.Stop()

	t := time.NewTimer(dur)

	<-t.C
}
