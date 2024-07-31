package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/congqixia/zangief/milvus"
	"github.com/congqixia/zangief/stat"
	"github.com/congqixia/zangief/stresser"
)

var (
	addr     = flag.String("addr", "", "service instance")
	token    = flag.String("token", "", "service auth token")
	username = flag.String("username", "", "username of service")
	password = flag.String("password", "", "password of service")
	collName = flag.String("collection", "", "collection name to press")
	dim      = flag.Int("dim", 768, "manual specifying vector dimension")

	reqType = flag.String("reqType", "grpc", "service request type")
	topK    = flag.Int("topk", 1000, "search topK value, default 1000")
	filter  = flag.String("filter", "", "search/query filter expression")

	workNum        = flag.Int("workNum", 100, "workerNum, default 100")
	interval       = flag.Duration("interval", time.Millisecond*100, "interval")
	tokens         = flag.Int("tokens", 10, "tokens per interval")
	periodPerEpoch = flag.Int("periodPerEpoch", 300, "period number per epoch, default 300 (300*interval per epoch, default 30s)")
	totalTime      = flag.Duration("totalTime", time.Minute*10, "total pressure time, default 10min")

	pooling = flag.Bool("pooling", true, "client pooling, default true")

	customTransport     = flag.Bool("customTransport", false, "use custom transport")
	maxConnsPerHost     = flag.Int("maxConnsPerHost", 100, "max connections per host")
	maxIdleConnsPerHost = flag.Int("maxIdleConnsPerHost", 150, "max idle connections per host")
	forceAttemptH2      = flag.Bool("forceAttemptH2", true, "force attemp http2")
	disableKeepAlives   = flag.Bool("disableKeepAlives", false, "disable keepalives")

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

	// grpc
	opts = append(opts, milvus.WithPooling(*pooling))
	// http
	opts = append(opts, milvus.WithCustomTransport(*customTransport))
	opts = append(opts, milvus.WithMaxConnsPerHost(*maxConnsPerHost))
	opts = append(opts, milvus.WithMaxIdleConnsPerHost(*maxIdleConnsPerHost))
	opts = append(opts, milvus.WithForceAttempHTTP2(*forceAttemptH2))
	opts = append(opts, milvus.WithDisableKeepAlives(*disableKeepAlives))

	w := milvus.NewMilvusSearchWorker(*addr, *collName, opts...)

	log.Println("Start Setup test information")

	var err error
	switch *reqType {
	case "grpc", "grpc-query":
		err = w.SetupCollectionInfo(context.Background())
	case "restful":
		w.PrintHttpClientInfo()
		err = w.SetupRandomTestVectors(*dim)
		//TODO setup collection info via restful API
	case "dry":
		err = w.SetupCollectionInfo(context.Background())
		if err != nil {
			log.Fatal("failed to setup test information: ", err.Error())
		}
		return
	}
	if err != nil {
		log.Fatal("failed to setup test information: ", err.Error())
	}

	log.Println("start stress testing")

	stress(func(e *stat.Epoch) {
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

func stress(w func(*stat.Epoch), dur time.Duration) {
	p := stresser.NewStresser(*interval, *workNum, *tokens, *periodPerEpoch, w)

	p.Start()
	defer p.Stop()

	t := time.NewTimer(dur)

	<-t.C
}
