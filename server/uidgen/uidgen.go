package uidgen

import (
	"context"
	"errors"
	"fmt"
	"github.com/hashicorp/golang-lru/v2"
	"log"
	"strconv"
	"sync"
	"time"
	"unique-id-generator/server/flusher"
	"unique-id-generator/server/flusherplane"
	"unique-id-generator/server/persistence"
)

var ErrMaxLimitReached = errors.New("reached maximum for the day")

type Metadata struct {
	mu *sync.Mutex

	*persistence.Counter
}

type UIDGen struct {
	cache                *lru.Cache[string, *Metadata]
	persistence          *persistence.Persistence
	fp                   *flusherplane.FlusherPlane
	invalidateBucketChan <-chan flusher.InvalidateBucketMessage
	updateETagChan       <-chan flusher.UpdateETagMessage
	logger               *log.Logger
}

func New(
	cache *lru.Cache[string, *Metadata],
	persistence *persistence.Persistence,
	fp *flusherplane.FlusherPlane,
	invalidateBucketChan <-chan flusher.InvalidateBucketMessage,
	updateETagChan <-chan flusher.UpdateETagMessage,
	logger *log.Logger,
) *UIDGen {
	gen := &UIDGen{
		cache:                cache,
		persistence:          persistence,
		fp:                   fp,
		invalidateBucketChan: invalidateBucketChan,
		updateETagChan:       updateETagChan,
		logger:               logger,
	}

	go gen.listenInvalidateBucketMsg()
	go gen.listenUpdateETagMsg()
	return gen
}

func (u *UIDGen) listenInvalidateBucketMsg() {
	for msg := range u.invalidateBucketChan {
		md, ok := u.cache.Get(msg.BucketId)
		if ok {
			md.mu.Lock()
			go func() {
				counter, err := u.persistence.GetCounter(context.Background(), msg.BucketId)
				switch err {
				case nil:
					u.logger.Printf("[FLUSH] retrieved latest counter [bucketId: %s]\n", msg.BucketId)
					md.Counter = counter
				default:
					u.logger.Printf("[FLUSH] failed to get counter, dumping counter from cache... [bucketId: %s]\n", msg.BucketId)
					u.cache.Remove(msg.BucketId)
				}
				md.mu.Unlock()
			}()
		}
	}
}

func (u *UIDGen) listenUpdateETagMsg() {
	for msg := range u.updateETagChan {
		md, ok := u.cache.Get(msg.BucketId)
		if ok {
			md.mu.Lock()
			md.ETag = msg.ETag
			md.mu.Unlock()
		}
	}
}

func (u *UIDGen) Generate(bucketId string) (uint64, error) {
	md, ok := u.cache.Get(bucketId)
	if !ok {
		u.logger.Printf("cache miss for bucketId: %s\n", bucketId)
		// Read from cosmos.
		md = &Metadata{
			mu:      &sync.Mutex{},
			Counter: nil,
		}

		u.logger.Printf("writing empty metadata on cache for bucketId: %s\n", bucketId)
		if peekMd, found, _ := u.cache.PeekOrAdd(bucketId, md); found {
			u.logger.Printf("found entry on cache peek for bucketId: %s\n", bucketId)
			md = peekMd
			md.mu.Lock() // Pauses racing goroutines.
			// TODO: Handle scenario where md is removed from cache.
		} else {
			// Newly written.
			u.logger.Printf("found newly created entry on cache peek for bucketId: %s\n", bucketId)
			md.mu.Lock() // Pauses racing goroutines.

			counter, err := u.persistence.GetCounter(context.Background(), bucketId)
			switch err {
			case nil:
				md.Counter = counter
			case persistence.ErrCounterNotFound:
				md.Counter = &persistence.Counter{
					Id:        bucketId,
					Counter:   0,
					Timestamp: time.Now(),
					ETag:      "",
				}
			default:
				u.cache.Remove(bucketId)
				return 0, err
			}
		}
	} else {
		u.logger.Printf("cache pass for bucketId %s\n", bucketId)
		md.mu.Lock()
	}

	if md.Counter.Counter >= 999999 {
		return 0, ErrMaxLimitReached
	}
	if ok := isSameDate(md.Timestamp, time.Now()); ok {
		md.Counter.Counter++
		log.Printf("incremented counter for bucketId: %s\n", bucketId)
	} else {
		md.Timestamp = time.Now()
		md.Counter.Counter = 1
		log.Printf("reset counter for bucketId: %s\n", bucketId)
	}

	counter := md.Counter.Counter
	u.fp.Add(bucketId, md.Counter)
	md.mu.Unlock()

	yyyy, mm, dd := md.Timestamp.Date()
	uid, err := strconv.Atoi(fmt.Sprintf("%d%02d%02d%06d", yyyy, mm, dd, counter))
	if err != nil {
		return 0, err
	}

	return uint64(uid), nil
}

func isSameDate(t1, t2 time.Time) bool {
	return t1.UTC().Truncate(time.Hour * 24).Equal(t2.UTC().Truncate(time.Hour * 24))
}

// TODO: Handle stale cached counter
// TODO: Another node could have a stale counter for the bucket (who may again end up being the handler), handle it
