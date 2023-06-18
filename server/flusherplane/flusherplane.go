package flusherplane

import (
	"context"
	"fmt"
	"hash/maphash"
	"log"
	"math/rand"
	"time"
	"unique-id-generator/server/flusher"
	"unique-id-generator/server/persistence"
)

type flusherExt struct {
	flusher       *flusher.Flusher
	cancelFunc    func()
	ctxCancelFunc context.CancelFunc
}

type FlusherPlane struct {
	flushers []*flusherExt
	seed     maphash.Seed
}

func randInRange(min, max int) int {
	source := rand.NewSource(time.Now().UnixMicro())
	n := rand.New(source).Intn(max-min) + min
	return n
}

func New(size int, p *persistence.Persistence, invalidateBucketChan chan<- flusher.InvalidateBucketMessage, updateETagChan chan<- flusher.UpdateETagMessage, logger *log.Logger) *FlusherPlane {
	fp := &FlusherPlane{
		flushers: make([]*flusherExt, size),
		seed:     maphash.MakeSeed(),
	}

	for i := 0; i < size; i++ {
		f := flusher.New(fmt.Sprintf("f%d", i), p, invalidateBucketChan, updateETagChan, logger)
		ctx, cancel := context.WithCancel(context.Background())
		fext := &flusherExt{
			flusher:       f,
			cancelFunc:    nil,
			ctxCancelFunc: cancel,
		}
		fext.cancelFunc = func() {
			fext.ctxCancelFunc()
		}

		fp.flushers[i] = fext
		go func() {
			sx := randInRange(60, 120)
			t := time.NewTimer(time.Second * time.Duration(sx))
			for {
				select {
				case <-ctx.Done():
					ctx, cancel = context.WithCancel(context.Background())
					fext.ctxCancelFunc = cancel

					if fext.flusher.Len() > 0 {
						fext.flusher.Flush()
					}

					sx := randInRange(60, 120)
					t.Reset(time.Second * time.Duration(sx))
				case <-t.C:
					if fext.flusher.Len() > 0 {
						fext.flusher.Flush()
					}

					sx := randInRange(60, 120)
					t.Reset(time.Second * time.Duration(sx))
				}
			}
		}()
	}

	return fp
}

func (fp *FlusherPlane) Add(bucketId string, counter *persistence.Bucket) {
	hash := maphash.String(fp.seed, bucketId)
	idx := hash % uint64(len(fp.flushers))
	fext := fp.flushers[idx]
	if fext.flusher.Len() > 5 {
		fext.cancelFunc() // Signal for flush.
	}

	fp.flushers[idx].flusher.Add(bucketId, counter)
}
