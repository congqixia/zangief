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
	addr string
	// token          string
	collectionName string

	vectors []entity.FloatVector

	opt        *option
	collection *collectionInfo

	pool *sync.Pool
}

type option struct {
	restfulPath string
	token       string
	username    string
	password    string
}

func WithRestfulPath(path string) MilvusOption {
	return func(opt *option) {
		opt.restfulPath = path
	}
}

func WithToken(token string) MilvusOption {
	return func(opt *option) {
		opt.token = token
	}
}

func WithPassword(username string) MilvusOption {
	return func(opt *option) {
		opt.username = username
	}
}

func WithUsername(password string) MilvusOption {
	return func(opt *option) {
		opt.password = password
	}
}

type MilvusOption func(opt *option)

func defaultOption() *option {
	return &option{
		restfulPath: "/v1/vector/search",
	}
}

func NewMilvusSearchWorker(addr string, collectionName string, opts ...MilvusOption) *MilvusSearchWorker {
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
		collectionName: collectionName,

		opt: opt,

		vectors: vectors,
		pool: &sync.Pool{
			New: func() any {
				ctx := context.Background()
				c, err := client.NewClient(ctx, client.Config{
					Address:  addr,
					APIKey:   opt.token,
					Username: opt.username,
					Password: opt.password,
				})

				if err != nil {
					log.Fatal("failed to connect to milvus instance")
				}
				return c
			},
		},
	}
}

// use milvus client to setup collection search information
func (w *MilvusSearchWorker) SetupCollectionInfo(ctx context.Context) error {
	c := w.pool.Get().(client.Client)
	collection, err := c.DescribeCollection(ctx, w.collectionName)
	if err != nil {
		return err
	}

	info := &collectionInfo{
		collection: collection,
	}

	// scan field to find pk field & vector field
	var vfc, pkfc int
	for _, field := range collection.Schema.Fields {
		if field.DataType == entity.FieldTypeFloatVector || field.DataType == entity.FieldTypeBinaryVector {
			info.vectorField = field
			vfc++
		}
		if field.PrimaryKey {
			info.pkField = field
			pkfc++
		}
	}

	if info.pkField != nil {
		log.Printf("Primary Key Field found: %s, data type: %s\n", info.pkField.Name, info.pkField.DataType.String())
	}
	if info.vectorField != nil {
		log.Printf("Vector Field found: %s, data type: %s\n", info.vectorField.Name, info.vectorField.DataType.String())

		idxes, err := c.DescribeIndex(ctx, w.collectionName, info.vectorField.Name)
		if err != nil {
			return err
		}
		for _, idx := range idxes {
			params := idx.Params()
			info.metricsType = entity.MetricType(params["metric_type"])
			log.Printf("Index metrics type found: %s\n", info.metricsType)
		}
	}

	// store parsed info for search/query usage
	w.collection = info
	return nil
}

func (w *MilvusSearchWorker) SearchGrpc(topK int, filter string) (time.Duration, error) {
	sp, _ := entity.NewIndexAUTOINDEXSearchParam(1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := w.pool.Get().(client.Client)
	defer w.pool.Put(c)
	vector := w.vectors[rand.Intn(1000)]
	start := time.Now()
	_, err := c.Search(ctx,
		w.collectionName, nil, filter, nil, []entity.Vector{entity.FloatVector(vector)}, w.collection.vectorField.Name, w.collection.metricsType, topK, sp, client.WithSearchQueryConsistencyLevel(entity.ClBounded))
	if err != nil {
		return time.Since(start), err
	}
	dur := time.Since(start)
	return dur, nil
}

func (opt *option) getBearer() string {
	if opt.token != "" {
		return opt.token
	}
	if opt.username != "" && opt.password != "" {
		return fmt.Sprintf("%s:%s", opt.username, opt.password)
	}
	return ""
}

func (w *MilvusSearchWorker) SearchRestful(topK int, filter string) (time.Duration, error) {
	url := fmt.Sprintf("%s%s", w.addr, w.opt.restfulPath)

	vs, _ := json.Marshal(w.vectors[rand.Intn(1000)])

	var jsonStr = []byte(fmt.Sprintf(`{"collectionName":"%s","vector":%v, "limit": %d}`, w.collectionName, string(vs), topK))
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	req.Header.Set("Content-Type", "application/json")
	if bearer := w.opt.getBearer(); bearer != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", bearer))
	}

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

func (w *MilvusSearchWorker) QueryGrpc(limit int64, filter string) (time.Duration, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := w.pool.Get().(client.Client)
	defer w.pool.Put(c)
	start := time.Now()
	_, err := c.Query(ctx, w.collectionName, nil, filter, []string{w.collection.pkField.Name}, client.WithLimit(limit))
	if err != nil {
		return time.Since(start), err
	}
	dur := time.Since(start)
	return dur, nil
}
