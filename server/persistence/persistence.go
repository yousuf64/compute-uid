package persistence

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"log"
	"net/http"
	"time"
)

var ErrBucketNotFound = errors.New("bucket not found")
var ErrETagNotMatched = errors.New("etag not matched")

type Bucket struct {
	BucketId  string      `json:"id"`
	Counter   uint64      `json:"counter"`
	Timestamp time.Time   `json:"timestamp"`
	ETag      azcore.ETag `json:"_etag"`
}

type bucketTyp struct {
	Id        string `json:"id"`
	Counter   uint64 `json:"counter"`
	Timestamp int64  `json:"timestamp"`
	ETag      string `json:"_etag"`
}

type Persistence struct {
	countersClient *azcosmos.ContainerClient
	logger         *log.Logger
}

func New(countersClient *azcosmos.ContainerClient, logger *log.Logger) *Persistence {
	return &Persistence{
		countersClient: countersClient,
		logger:         logger,
	}
}

func (p *Persistence) GetBucket(ctx context.Context, bucketId string) (*Bucket, error) {
	p.logger.Printf("[PERSISTENCE] Fetching bucket BucketId: %s", bucketId)
	response, err := p.countersClient.ReadItem(ctx, azcosmos.NewPartitionKeyString(bucketId), bucketId, nil)
	if err != nil {
		var responseErr *azcore.ResponseError
		errors.As(err, &responseErr)
		if responseErr != nil && responseErr.StatusCode == http.StatusNotFound {
			p.logger.Printf("bucket %s not found", bucketId)
			return nil, ErrBucketNotFound
		}
		return nil, err
	}

	var bkt *bucketTyp
	err = json.NewDecoder(response.RawResponse.Body).Decode(&bkt)
	if err != nil {
		p.logger.Printf("[PERSISTENCE] Decoding bucket failed BucketId: %s", bucketId)
		return nil, err
	}
	return &Bucket{
		BucketId:  bkt.Id,
		Counter:   bkt.Counter,
		Timestamp: time.Unix(bkt.Timestamp, 0),
		ETag:      azcore.ETag(bkt.ETag),
	}, nil
}

func (p *Persistence) UpsertBucket(ctx context.Context, bucketId string, bucket *Bucket, options *azcosmos.ItemOptions) (etag azcore.ETag, err error) {
	bkt := &bucketTyp{
		Id:        bucket.BucketId,
		Counter:   bucket.Counter,
		Timestamp: bucket.Timestamp.UnixMilli(),
		ETag:      "",
	}

	b, err := json.Marshal(bkt)
	if err != nil {
		p.logger.Printf("[PERSISTENCE] Encoding bucket failed BucketId: %s", bucketId)
		return etag, err
	}
	p.logger.Printf("[PERSISTENCE] Upserting bucket BucketId: %s", bucketId)
	resp, err := p.countersClient.UpsertItem(ctx, azcosmos.NewPartitionKeyString(bucketId), b, options)
	etag = resp.ETag
	switch err {
	case nil:
		return
	case context.Canceled:
		p.logger.Printf("[PERSISTENCE] Bucket upsert cancelled BucketId: %s", bucketId)
		return
	default:
		var responseErr *azcore.ResponseError
		errors.As(err, &responseErr)
		if responseErr != nil && responseErr.StatusCode == http.StatusPreconditionFailed {
			p.logger.Printf("[PERSISTENCE] Bucket ETag not matched BucketId: %s, ETag: %s", bucketId, string(*options.IfMatchEtag))
			return etag, ErrETagNotMatched
		}
		p.logger.Printf("[PERSISTENCE] Bucket upsert failed BucketId: %s, Err: %s", bucketId, err)
		return etag, err
	}
}
