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
	p.logger.Printf("fetching bucket %s", bucketId)
	response, err := p.countersClient.ReadItem(ctx, azcosmos.NewPartitionKeyString(bucketId), bucketId, nil)
	if err != nil {
		var responseErr *azcore.ResponseError
		errors.As(err, &responseErr)
		if responseErr != nil && responseErr.StatusCode == http.StatusNotFound {
			p.logger.Printf("bucket %s not found", bucketId)
			return nil, ErrBucketNotFound
		}
		return nil, err
	} else {
		var bkt *bucketTyp
		err = json.NewDecoder(response.RawResponse.Body).Decode(&bkt)
		if err != nil {
			p.logger.Printf("decoding bucket %s failed", bucketId)
			return nil, err
		}
		return &Bucket{
			BucketId:  bkt.Id,
			Counter:   bkt.Counter,
			Timestamp: time.Unix(bkt.Timestamp, 0),
			ETag:      azcore.ETag(bkt.ETag),
		}, nil
	}

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
		p.logger.Printf("encoding bucket failed [bucketId: %s]", bucketId)
		return etag, err
	}
	p.logger.Printf("upserting bucket [bucketId: %s]", bucketId)
	resp, err := p.countersClient.UpsertItem(ctx, azcosmos.NewPartitionKeyString(bucketId), b, options)
	etag = resp.ETag
	switch err {
	case nil:
		return
	case context.Canceled:
		p.logger.Printf("bucket upsert cancelled [bucketId: %s]", bucketId)
		return
	default:
		var responseErr *azcore.ResponseError
		errors.As(err, &responseErr)
		if responseErr != nil && responseErr.StatusCode == http.StatusPreconditionFailed {
			p.logger.Printf("bucket etag not matched [bucketId: %s, etag: %s]", bucketId, string(*options.IfMatchEtag))
			return etag, ErrETagNotMatched
		}
		p.logger.Printf("bucket upsert failed [bucketId: %s]", bucketId)
		return etag, err
	}
}
