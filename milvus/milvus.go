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

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"go.uber.org/atomic"
)

type MilvusSearchWorker struct {
	addr           string
	collectionName string

	vectors []entity.FloatVector

	opt        *option
	collection *collectionInfo

	c       client.Client
	once    sync.Once
	clients []client.Client
	idx     atomic.Int32
}

type option struct {
	restfulPath string
	token       string
	username    string
	password    string

	// grpc
	pooling bool

	// http client
	customTransport     bool
	maxIdleConnsPerHost int
	maxConnsPerHost     int
	forceAttemptH2      bool
	disableKeepAlives   bool
}

func NewMilvusSearchWorker(addr string, collectionName string, opts ...MilvusOption) *MilvusSearchWorker {
	opt := defaultOption()

	for _, o := range opts {
		o(opt)
	}

	clients := make([]client.Client, 0, 100)
	for i := 0; i < 100; i++ {
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
		clients = append(clients, c)
	}

	return &MilvusSearchWorker{
		addr:           addr,
		collectionName: collectionName,

		opt:     opt,
		clients: clients,
	}
}

// setupRandomTestVectors setup random vectors
func (w *MilvusSearchWorker) SetupRandomTestVectors(dim int) error {
	if dim <= 0 {
		return errors.Newf("invalid dim: %d", dim)
	}
	// TODO support setup other vectors, say binary, fp16, etc.
	vectors := make([]entity.FloatVector, 0, 1000)
	for i := 0; i < 1000; i++ {
		vector := make([]float32, 0, dim)
		for j := 0; j < 768; j++ {
			vector = append(vector, rand.Float32())
		}
		vectors = append(vectors, entity.FloatVector(vector))
	}
	w.vectors = vectors
	return nil
}

func (w *MilvusSearchWorker) PrintHttpClientInfo() {
	if !w.opt.customTransport {
		log.Println("Using default http transport")
		return
	}
	log.Println("Using customized http transport")
	log.Println("MaxConnsPerHost: ", w.opt.maxConnsPerHost)
	log.Println("MaxIdleConnsPerHost: ", w.opt.maxIdleConnsPerHost)
	log.Println("ForceAttemptHTTP2: ", w.opt.forceAttemptH2)
	log.Println("DisableKeepAlives: ", w.opt.disableKeepAlives)
	log.Println()
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
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer := w.opt.getBearer(); bearer != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", bearer))
	}

	start := time.Now()

	client := &http.Client{}
	if w.opt.customTransport {
		trans := http.DefaultTransport.(*http.Transport)
		trans.MaxIdleConnsPerHost = w.opt.maxIdleConnsPerHost
		trans.MaxConnsPerHost = w.opt.maxConnsPerHost
		trans.ForceAttemptHTTP2 = w.opt.forceAttemptH2
		trans.DisableKeepAlives = w.opt.disableKeepAlives
		client.Transport = trans
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
