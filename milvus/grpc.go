package milvus

import (
	"context"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
)

// use milvus client to setup collection search information
func (w *MilvusSearchWorker) SetupCollectionInfo(ctx context.Context) error {
	c := w.getClient()
	defer w.putClient(c)
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

		dimStr, ok := info.vectorField.TypeParams["dim"]
		if ok {
			dim, err := strconv.Atoi(dimStr)
			if err != nil {
				return errors.Newf("failed to parse vector dim: %w", err)
			}
			info.dim = dim
		} else {
			return errors.New("not dim found in vector field schema")
		}

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

	return w.SetupRandomTestVectors(w.collection.dim)
}

func (w *MilvusSearchWorker) getClient() client.Client {
	if w.opt.pooling {
		idx := w.idx.Inc()
		return w.clients[int(idx)%len(w.clients)]
	}

	w.once.Do(func() {
		ctx := context.Background()
		c, err := client.NewClient(ctx, client.Config{
			Address:  w.addr,
			APIKey:   w.opt.token,
			Username: w.opt.username,
			Password: w.opt.password,
		})

		if err != nil {
			log.Fatal("failed to connect to milvus instance: ", err.Error())
		}
		w.c = c
	})
	return w.c
}

func (w *MilvusSearchWorker) putClient(c client.Client) {
	// if w.opt.pooling {
	// 	w.pool.Put(c)
	// }
}

func (w *MilvusSearchWorker) SearchGrpc(topK int, filter string) (time.Duration, error) {
	sp, _ := entity.NewIndexAUTOINDEXSearchParam(1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := w.getClient()
	defer w.putClient(c)
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

func (w *MilvusSearchWorker) QueryGrpc(limit int64, filter string) (time.Duration, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := w.getClient()
	defer w.putClient(c)
	start := time.Now()
	_, err := c.Query(ctx, w.collectionName, nil, filter, []string{w.collection.pkField.Name}, client.WithLimit(limit))
	if err != nil {
		return time.Since(start), err
	}
	dur := time.Since(start)
	return dur, nil
}
