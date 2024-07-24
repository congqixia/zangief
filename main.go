package main

import (
	"context"
	"flag"
	"time"

	"github.com/congqixia/zangief/milvus"
	"github.com/congqixia/zangief/presser"
	"github.com/congqixia/zangief/stat"
)

var (
	addr     = flag.String("addr", "", "service instance")
	token    = flag.String("token", "", "service auth token")
	username = flag.String("username", "", "username of service")
	password = flag.String("password", "", "password of service")
	collName = flag.String("collection", "", "collection name to press")

	reqType = flag.String("reqType", "grpc", "service request type")
	topK    = flag.Int("topk", 1000, "search topK value, default 1000")
	filter  = flag.String("filter", "", "search/query filter expression")

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
	if *token != "" {
		opts = append(opts, milvus.WithToken(*token))
	}
	if *username != "" {
		opts = append(opts, milvus.WithUsername(*username))
	}
	if *password != "" {
		opts = append(opts, milvus.WithPassword(*password))
	}

	w := milvus.NewMilvusSearchWorker(*addr, *collName, opts...)
	w.SetupCollectionInfo(context.Background())

	if *reqType == "dry" {
		return
	}

	doPress(func(e *stat.Epoch) {
		defer e.Done()
		var dur time.Duration
		var err error
		switch *reqType {
		case "grpc":
			dur, err = w.SearchGrpc(*topK, *filter)
		case "restful":
			dur, err = w.SearchRestful(*topK, *filter)
		case "grpc-query":
			dur, err = w.QueryGrpc(int64(*topK), *filter)
		}
		if err != nil {
			e.RecordError(err)
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
