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

var ErrCounterNotFound = errors.New("counter not found")
var ErrETagNotMatched = errors.New("etag not matched")

type Counter struct {
	BucketId  string      `json:"id"`
	Counter   uint64      `json:"counter"`
	Timestamp time.Time   `json:"timestamp"`
	ETag      azcore.ETag `json:"_etag"`
}

type counter struct {
	Id        string `json:"id"`
	Counter   uint64 `json:"counter"`
	Timestamp int64  `json:"_ts"`
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

func (p *Persistence) GetCounter(ctx context.Context, bucketId string) (*Counter, error) {
	p.logger.Printf("fetching counter %s", bucketId)
	response, err := p.countersClient.ReadItem(ctx, azcosmos.NewPartitionKeyString(bucketId), bucketId, nil)
	if err != nil {
		var responseErr *azcore.ResponseError
		errors.As(err, &responseErr)
		if responseErr != nil && responseErr.StatusCode == http.StatusNotFound {
			p.logger.Printf("counter %s not found", bucketId)
			return nil, ErrCounterNotFound
		}
		return nil, err
	} else {
		var ctr *counter
		err = json.NewDecoder(response.RawResponse.Body).Decode(&ctr)
		if err != nil {
			p.logger.Printf("decoding counter %s failed", bucketId)
			return nil, err
		}
		return &Counter{
			BucketId:  ctr.Id,
			Counter:   ctr.Counter,
			Timestamp: time.Unix(ctr.Timestamp, 0),
			ETag:      azcore.ETag(ctr.ETag),
		}, nil
	}

}

func (p *Persistence) SaveCounter(ctx context.Context, bucketId string, counter *Counter, options *azcosmos.ItemOptions) (etag azcore.ETag, err error) {
	b, err := json.Marshal(counter)
	if err != nil {
		p.logger.Printf("encoding counter failed [bucketId: %s]", bucketId)
		return etag, err
	}
	p.logger.Printf("upserting counter [bucketId: %s]", bucketId)
	resp, err := p.countersClient.UpsertItem(ctx, azcosmos.NewPartitionKeyString(bucketId), b, options)
	etag = resp.ETag
	switch err {
	case nil:
		return
	case context.Canceled:
		p.logger.Printf("counter upsert cancelled [bucketId: %s]", bucketId)
		return
	default:
		var responseErr *azcore.ResponseError
		errors.As(err, &responseErr)
		if responseErr != nil && responseErr.StatusCode == http.StatusPreconditionFailed {
			p.logger.Printf("counter etag not matched [bucketId: %s, etag: %s]", bucketId, string(*options.IfMatchEtag))
			return etag, ErrETagNotMatched
		}
		p.logger.Printf("counter upsert failed [bucketId: %s]", bucketId)
		return etag, err
	}
}
