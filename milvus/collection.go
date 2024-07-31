package milvus

import "github.com/milvus-io/milvus-sdk-go/v2/entity"

// collectionInfo contains milvus collection parsed information prepared for
type collectionInfo struct {
	collection *entity.Collection

	pkField     *entity.Field
	vectorField *entity.Field

	metricsType entity.MetricType

	dim int
}
