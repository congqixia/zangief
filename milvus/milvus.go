package milvus

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
)

type MilvusSearchWorker struct {
	addr           string
	token          string
	collectionName string

	vectors []entity.FloatVector

	opt *option

	pool *sync.Pool
}

type option struct {
	restfulPath string
}

func WithRestfulPath(path string) MilvusOption {
	return func(opt *option) {
		opt.restfulPath = path
	}
}

type MilvusOption func(opt *option)

func defaultOption() *option {
	return &option{
		restfulPath: "/v1/vector/search",
	}
}

func NewMilvusSearchWorker(addr string, token string, collectionName string, opts ...MilvusOption) *MilvusSearchWorker {
	vectors := make([]entity.FloatVector, 0, 1000)
	for i := 0; i < 1000; i++ {
		vector := make([]float32, 0, 768)
		for j := 0; j < 768; j++ {
			vector = append(vector, rand.Float32())
		}
		vectors = append(vectors, entity.FloatVector(vector))
	}

	opt := defaultOption()

	for _, o := range opts {
		o(opt)
	}

	return &MilvusSearchWorker{
		addr:           addr,
		token:          token,
		collectionName: collectionName,

		opt: opt,

		vectors: vectors,
		pool: &sync.Pool{
			New: func() any {
				ctx := context.Background()
				c, err := client.NewClient(ctx, client.Config{
					Address: addr,
					APIKey:  token,
				})

				if err != nil {
					log.Fatal("failed to connect to milvus instance")
				}
				return c
			},
		},
	}
}

func (w *MilvusSearchWorker) SearchGrpc(topK int) (time.Duration, error) {
	sp, _ := entity.NewIndexAUTOINDEXSearchParam(1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := w.pool.Get().(client.Client)
	defer w.pool.Put(c)
	vector := w.vectors[rand.Intn(1000)]
	start := time.Now()
	_, err := c.Search(ctx, w.collectionName, nil, "", nil, []entity.Vector{entity.FloatVector(vector)}, "vector", entity.L2, topK, sp)
	if err != nil {
		return time.Since(start), err
	}
	dur := time.Since(start)
	return dur, nil
}

func (w *MilvusSearchWorker) SearchRestful(topK int) (time.Duration, error) {
	url := fmt.Sprintf("%s%s", w.addr, w.opt.restfulPath)

	vs, _ := json.Marshal(w.vectors[rand.Intn(1000)])

	var jsonStr = []byte(fmt.Sprintf(`{"collectionName":"%s","vector":%v, "limit": %d}`, w.collectionName, string(vs), topK))
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", w.token))

	start := time.Now()
	trans := http.DefaultTransport.(*http.Transport)
	trans.MaxIdleConnsPerHost = 100
	trans.ForceAttemptHTTP2 = false

	client := &http.Client{
		Transport: trans,
	}
	resp, err := client.Do(req)
	if err != nil {
		log.Println(err)
		return time.Since(start), err
	}
	defer resp.Body.Close()

	_, _ = io.ReadAll(resp.Body)
	return time.Since(start), nil
}
