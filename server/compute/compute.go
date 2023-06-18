package compute

import (
	"context"
	"errors"
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
	"log"
	"strconv"
	"sync"
	"time"
	"unique-id-generator/server/channels"
	"unique-id-generator/server/messages"
	"unique-id-generator/server/persistence"
)

var ErrMaxLimitReached = errors.New("reached maximum for the day")

type CacheBucket struct {
	Counter   uint64
	Timestamp time.Time
	fetched   bool
	mu        *sync.Mutex
}

type Compute struct {
	id          string
	cache       *lru.Cache[string, *CacheBucket]
	mu          *sync.Mutex
	persistence *persistence.Persistence
	logger      *log.Logger
}

func New(id string, persistence *persistence.Persistence, logger *log.Logger) *Compute {
	cache, err := lru.New[string, *CacheBucket](100)
	if err != nil {
		log.Fatal(err)
	}

	return &Compute{
		id:          id,
		cache:       cache,
		mu:          &sync.Mutex{},
		persistence: persistence,
		logger:      logger,
	}
}

func (c *Compute) Id() string {
	return c.id
}

func (c *Compute) ComputeId(bucketId string) (uint64, error) {
	bucket := c.bucketFromCache(bucketId)
	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	if !bucket.fetched {
		bkt, err := c.persistence.GetBucket(context.Background(), bucketId)
		switch err {
		case nil:
			c.logger.Printf("[Compute %s] Fetched bucket from persistence BucketId: %s\n", c.id, bucketId)
			bucket.fetched = true
			bucket.Counter = bkt.Counter
			bucket.Timestamp = bkt.Timestamp
			c.logger.Printf("[Compute %s] Sending UpdateETagMessage... BucketId: %s, ETag: %s\n", c.id, bucketId, bkt.ETag)
			channels.UpdateETag <- &messages.UpdateETagMessage{
				BucketId: bucketId,
				ETag:     bkt.ETag,
			}
		case persistence.ErrBucketNotFound:
			bucket.fetched = true
			c.logger.Printf("[Compute %s] Bucket not found in persistence BucketId: %s\n", c.id, bucketId)
			c.logger.Printf("[Compute %s] Sending UpdateETagMessage... BucketId: %s, ETag: %s\n", c.id, bucketId, "<empty>")
			channels.UpdateETag <- &messages.UpdateETagMessage{
				BucketId: bucketId,
				ETag:     "",
			}
		default:
			c.logger.Printf("[Compute %s] Error fetching bucket from persistence BucketId: %s, Error: %s\n", c.id, bucketId, err)
			c.cache.Remove(bucketId)
			return 0, err
		}
	}

	if bucket.Counter >= 999999 {
		return 0, ErrMaxLimitReached
	}
	if ok := isSameDate(bucket.Timestamp, time.Now()); ok {
		bucket.Counter++
		c.logger.Printf("[Compute %s] Incremented counter BucketId: %s\n", c.id, bucketId)
	} else {
		bucket.Timestamp = time.Now()
		bucket.Counter = 1
		c.logger.Printf("[Compute %s] Reset counter BucketId: %s\n", c.id, bucketId)
	}

	c.logger.Printf("[Compute %s] Sending UpdateCounterMessage BucketId: %s, Counter: %d\n", c.id, bucketId, bucket.Counter)
	c.publishUpdateCounter(bucketId, bucket.Counter, bucket.Timestamp)
	return generateId(bucket.Timestamp, bucket.Counter)
}

func (c *Compute) publishUpdateCounter(bucketId string, counter uint64, timestamp time.Time) {
	channels.UpdateCounter <- &messages.UpdateCounterMessage{
		BucketId:  bucketId,
		Counter:   counter,
		Timestamp: timestamp,
	}
}

func (c *Compute) InvalidateBucket(msg *messages.InvalidateBucketMessage) {
	c.logger.Printf("[Compute %s] Invoking InvalidateBucketMessage. Attempting to invalidate bucket... BucketId: %s\n", c.id, msg.BucketId)
	bucket := c.bucketFromCache(msg.BucketId)
	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	bkt, err := c.persistence.GetBucket(context.Background(), msg.BucketId)
	switch err {
	case nil:
		c.logger.Printf("[Compute %s] Fetched bucket from persistence BucketId: %s\n", c.id, msg.BucketId)
		bucket.fetched = true
		bucket.Counter = bkt.Counter
		bucket.Timestamp = bkt.Timestamp
		c.logger.Printf("[Compute %s] Sending UpdateCounterMessage... BucketId: %s, Counter: %d\n", c.id, msg.BucketId, bkt.Counter)
		channels.UpdateCounter <- &messages.UpdateCounterMessage{
			BucketId:  bkt.BucketId,
			Counter:   bkt.Counter,
			Timestamp: bkt.Timestamp,
		}
		c.logger.Printf("[Compute %s] Sending UpdateETagMessage... BucketId: %s, ETag: %s\n", c.id, msg.BucketId, bkt.ETag)
		channels.UpdateETag <- &messages.UpdateETagMessage{
			BucketId: bkt.BucketId,
			ETag:     bkt.ETag,
		}
	case persistence.ErrBucketNotFound:
		c.logger.Printf("[Compute %s] Bucket not found in persistence BucketId: %s\n", c.id, msg.BucketId)
		bucket.fetched = true
		bucket.Counter = 0
		bucket.Timestamp = time.Now()
		c.logger.Printf("[Compute %s] Sending UpdateCounterMessage... BucketId: %s, Counter: %d\n", c.id, msg.BucketId, bucket.Counter)
		channels.UpdateCounter <- &messages.UpdateCounterMessage{
			BucketId:  msg.BucketId,
			Counter:   bucket.Counter,
			Timestamp: bucket.Timestamp,
		}
		c.logger.Printf("[Compute %s] Sending UpdateETagMessage... BucketId: %s, ETag: %s\n", c.id, msg.BucketId, "<empty>")
		channels.UpdateETag <- &messages.UpdateETagMessage{
			BucketId: msg.BucketId,
			ETag:     "",
		}
	default:
		c.logger.Printf("[Compute %s] Error fetching bucket from persistence BucketId: %s, Error: %s\n", c.id, msg.BucketId, err)
		c.cache.Remove(msg.BucketId)
	}
}

func generateId(t time.Time, counter uint64) (uint64, error) {
	yyyy, mm, dd := t.Date()
	uid, err := strconv.Atoi(fmt.Sprintf("%d%02d%02d%06d", yyyy, mm, dd, counter))
	if err != nil {
		return 0, err
	}
	return uint64(uid), nil
}

func (c *Compute) bucketFromCache(bucketId string) *CacheBucket {
	value, ok := c.cache.Get(bucketId)
	if !ok {
		// cache miss
		c.logger.Printf("[Compute %s] Cache miss BucketId: %s\n", c.id, bucketId)
		value = &CacheBucket{
			Counter:   0,
			Timestamp: time.Now(),
			fetched:   false,
			mu:        &sync.Mutex{},
		}

		// race to add to the cache
		c.logger.Printf("[Compute %s] Racing to add to the cache BucketId: %s\n", c.id, bucketId)
		peekValue, found, _ := c.cache.PeekOrAdd(bucketId, value)
		if found {
			// lost the race, a different goroutine has added to the cache. use that value.
			c.logger.Printf("[Compute %s] Race lost BucketId: %s\n", c.id, bucketId)
			return peekValue
		} else {
			// won the race
			c.logger.Printf("[Compute %s] Race won BucketId: %s\n", c.id, bucketId)
			return value
		}
	}
	// cache hit
	c.logger.Printf("[Compute %s] Cache hit BucketId: %s\n", c.id, bucketId)
	return value
}

func isSameDate(t1, t2 time.Time) bool {
	return t1.UTC().Truncate(time.Hour * 24).Equal(t2.UTC().Truncate(time.Hour * 24))
}
