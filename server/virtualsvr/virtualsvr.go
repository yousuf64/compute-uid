package virtualsvr

import (
	"context"
	"errors"
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
	"log"
	"strconv"
	"sync"
	"time"
	"unique-id-generator/server/messages"
	"unique-id-generator/server/persistence"
	"unique-id-generator/server/streams"
)

var ErrMaxLimitReached = errors.New("reached maximum for the day")

type Metadata struct {
	mu *sync.Mutex

	*persistence.Counter
}

type VirtualSvr struct {
	cache       *lru.Cache[string, *Metadata]
	mu          *sync.Mutex
	persistence *persistence.Persistence
	logger      *log.Logger
}

func New(persistence *persistence.Persistence, logger *log.Logger) *VirtualSvr {
	cache, err := lru.New[string, *Metadata](100)
	if err != nil {
		log.Fatal(err)
	}

	return &VirtualSvr{
		cache:       cache,
		mu:          &sync.Mutex{},
		persistence: persistence,
		logger:      logger,
	}
}

func (vsvr *VirtualSvr) Serve(bucketId string) (uint64, error) {
	cacheCounter := vsvr.counterFromCache(bucketId)
	cacheCounter.mu.Lock()
	defer cacheCounter.mu.Unlock()

	if cacheCounter.Counter == nil {
		// call cosmos
		counter, err := vsvr.persistence.GetCounter(context.Background(), bucketId)
		switch err {
		case nil:
			cacheCounter.Counter = counter
			streams.UpdateETagMessage <- messages.UpdateETagMessage{
				BucketId: bucketId,
				ETag:     counter.ETag,
			}
		case persistence.ErrCounterNotFound:
			cacheCounter.Counter = &persistence.Counter{
				BucketId:  bucketId,
				Counter:   0,
				Timestamp: time.Now(),
				ETag:      "",
			}
			streams.UpdateETagMessage <- messages.UpdateETagMessage{
				BucketId: bucketId,
				ETag:     "",
			}
		default:
			vsvr.cache.Remove(bucketId)
			return 0, err
		}
	}

	if cacheCounter.Counter.Counter >= 999999 {
		return 0, ErrMaxLimitReached
	}
	if ok := isSameDate(cacheCounter.Timestamp, time.Now()); ok {
		cacheCounter.Counter.Counter++
		vsvr.logger.Printf("incremented counter for bucketId: %s\n", bucketId)
	} else {
		cacheCounter.Timestamp = time.Now()
		cacheCounter.Counter.Counter = 1
		vsvr.logger.Printf("reset counter for bucketId: %s\n", bucketId)
	}

	counter := cacheCounter.Counter.Counter
	streams.UpdateCounterMessage <- messages.UpdateCounterMessage{
		BucketId: cacheCounter.BucketId,
		Counter:  counter,
	}

	return generateId(cacheCounter.Timestamp, counter)
}

func generateId(t time.Time, counter uint64) (uint64, error) {
	yyyy, mm, dd := t.Date()
	uid, err := strconv.Atoi(fmt.Sprintf("%d%02d%02d%06d", yyyy, mm, dd, counter))
	if err != nil {
		return 0, err
	}
	return uint64(uid), nil
}

func (vsvr *VirtualSvr) counterFromCache(bucketId string) *Metadata {
	value, ok := vsvr.cache.Get(bucketId)
	if !ok {
		// cache miss
		value = &Metadata{
			mu:      &sync.Mutex{},
			Counter: nil,
		}

		// race to add to the cache
		peekValue, found, _ := vsvr.cache.PeekOrAdd(bucketId, value)
		if found {
			// lost the race, a different goroutine has added to the cache
			return peekValue
		} else {
			// won the race
			return value
		}
	}
	return value
}

func isSameDate(t1, t2 time.Time) bool {
	return t1.UTC().Truncate(time.Hour * 24).Equal(t2.UTC().Truncate(time.Hour * 24))
}
