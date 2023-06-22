package flusherv2

import (
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"log"
	"os"
	"sync"
	"unique-id-generator/server/channels"
	"unique-id-generator/server/messages"
	"unique-id-generator/server/persistence"
)

func Listen(p *persistence.Persistence, logger *log.Logger) {
	(&sync.Once{}).Do(func() {
		go func() {
			for m := range channels.FlushQueue {
				logger.Printf("[Flusher] Received FlushQueueMessage from QM: %s", m.QueueMapId)

				pass := 0
				fail := 0
				for bid, bdata := range m.Data {
					bucket := &persistence.Bucket{
						BucketId:  bid,
						Counter:   bdata.Counter,
						Timestamp: bdata.Timestamp,
						ETag:      bdata.ETag,
					}
					etag, err := p.UpsertBucket(context.Background(), bid, bucket, &azcosmos.ItemOptions{IfMatchEtag: &bdata.ETag})
					switch err {
					case nil:
						logger.Printf("[Flusher] Flush OK! BucketId: %s, Counter: %d\n", bid, bdata.Counter)
						channels.UpdateETag <- &messages.UpdateETagMessage{
							BucketId: bid,
							ETag:     etag,
						}
						logger.Printf("[Flusher] Dispatched UpdateETagMessage BucketId: %s, ETag: %d\n", bid, etag)
						pass++
					case context.Canceled:
						logger.Printf("[Flusher] Flush aborted! BucketId: %s, Counter: %d\n", bid, bdata.Counter)
						fail++
					case persistence.ErrETagNotMatched:
						logger.Printf("[Flusher] ETag conflict BucketId: %s, ETag: %s, Counter: %d\n", bid, bdata.ETag, bdata.Counter)
						channels.InvalidateBucket <- &messages.InvalidateBucketMessage{BucketId: bid}
						logger.Printf("[Flusher] Dispatched InvalidateBucketMessage BucketId: %s\n", bid)
						fail++
					default:
						logger.Printf("[Flusher] Flush failed! BucketId: %s, Counter: %d, Error: %s\n", bid, bdata.Counter, err)
						fail++
					}

				}

				err := os.Remove(m.Filename)
				if err != nil {
					logger.Printf("[Flusher] Error removing WAL file %s\n", m.Filename, err)
				} else {
					logger.Printf("[Flusher] Removed WAL file %s\n", m.Filename)
				}
				logger.Printf("[Flusher] Done! Pass: %d, Fail: %d, Total: %d\n", pass, fail, len(m.Data))
			}
		}()
	})
}
