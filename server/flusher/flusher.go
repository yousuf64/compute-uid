package flusher

import (
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/google/uuid"
	"log"
	"os"
	"sync"
	"unique-id-generator/server/persistence"
)

type InvalidateBucketMessage struct {
	BucketId string
}

type UpdateETagMessage struct {
	BucketId string
	ETag     azcore.ETag
}

type Flusher struct {
	id                   string
	file                 *os.File
	mu                   *sync.Mutex
	counters             map[string]*persistence.Bucket
	p                    *persistence.Persistence
	invalidateBucketChan chan<- InvalidateBucketMessage
	updateETagChan       chan<- UpdateETagMessage
	logger               *log.Logger
}

func New(id string, p *persistence.Persistence, invalidateBucketChan chan<- InvalidateBucketMessage, updateETagChan chan<- UpdateETagMessage, logger *log.Logger) *Flusher {
	return &Flusher{
		id:                   id,
		file:                 newLogFile(id),
		mu:                   &sync.Mutex{},
		counters:             make(map[string]*persistence.Bucket),
		p:                    p,
		invalidateBucketChan: invalidateBucketChan,
		updateETagChan:       updateETagChan,
		logger:               logger,
	}
}

func newLogFile(flusherId string) *os.File {
	fid, err := uuid.NewUUID()
	fname := fmt.Sprintf("flusherlogs/%s_%s", flusherId, fid.String())
	file, err := os.OpenFile(fname, os.O_CREATE, 0755)
	if err != nil {
		log.Fatal(err)
	}
	return file
}

func (f *Flusher) Add(bucketId string, counter *persistence.Bucket) {
	f.mu.Lock()
	f.counters[bucketId] = counter

	for i := 0; i < 3; i++ {
		_, err := f.file.Write([]byte(fmt.Sprintf("%s|%d|%s\n", bucketId, counter.Counter, counter.ETag)))
		if err != nil {
			f.logger.Printf("[Flusher] WAL write failed %v %v\n", bucketId, counter.Counter)
			if i < 2 {
				f.logger.Printf("[Flusher] Retrying... %v %v\n", bucketId, counter.Counter)
			}
			continue
		}
		break
	}

	f.logger.Printf("[Flusher] wrote %v %v\n", bucketId, counter.Counter)
	f.mu.Unlock()
}

func (f *Flusher) purge() {
	f.counters = make(map[string]*persistence.Bucket)
	f.file = newLogFile(f.id)
}

func (f *Flusher) Flush() {
	f.mu.Lock()

	f.logger.Printf("[FLUSH] preparing to flush\n")
	f.flush()

	f.purge()
	f.logger.Printf("[FLUSH] WAL swapped\n")
	f.mu.Unlock()
}

func (f *Flusher) flush() {
	counters := f.counters
	file := f.file
	fname := file.Name()

	go func() {
		f.logger.Printf("[FLUSH] starting flusher\n")
		for bucketId, counter := range counters {
			etag, err := f.p.UpsertBucket(context.Background(), bucketId, counter, &azcosmos.ItemOptions{IfMatchEtag: &counter.ETag})
			switch err {
			case nil:
				f.logger.Printf("[FLUSH] flushed successfully [bucketId: %s, counter: %d]\n", bucketId, counter.Counter)
				f.updateETagChan <- UpdateETagMessage{
					BucketId: bucketId,
					ETag:     etag,
				}
				f.logger.Printf("[FLUSH] dispatched update etag request [bucketId: %s, etag: %d]\n", bucketId, etag)
			case context.Canceled:
				f.logger.Printf("[FLUSH] flush aborted [bucketId: %s, counter: %d]\n", bucketId, counter.Counter)
			case persistence.ErrETagNotMatched:
				f.logger.Printf("[FLUSH] etag unmatched [bucketId: %s, etag: %s, counter: %d]\n", bucketId, counter.ETag, counter.Counter)
				f.invalidateBucketChan <- InvalidateBucketMessage{BucketId: bucketId}
				f.logger.Printf("[FLUSH] dispatched invalidate cache request [bucketId: %s]\n", bucketId)
			default:
				f.logger.Printf("[FLUSH] flush failed [bucketId: %s, counter: %d]\n", bucketId, counter.Counter)
			}
		}
		err := file.Close()
		if err != nil {
			f.logger.Printf("failed to close the file %s\n", fname)
		}
		err = os.Remove(fname)
		if err != nil {
			f.logger.Printf("failed to remove the file %s\n", fname)
		}
		f.logger.Printf("[FLUSH] flusher done\n")
	}()
}

func (f *Flusher) Len() int32 {
	return int32(len(f.counters))
}
