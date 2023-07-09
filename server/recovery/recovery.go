package recovery

import (
	"bufio"
	"context"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
	"unique-id-generator/server/persistence"
)

type Recovery struct {
	p      *persistence.Persistence
	logger *log.Logger
}

func New(p *persistence.Persistence, logger *log.Logger) *Recovery {
	return &Recovery{
		p:      p,
		logger: logger,
	}
}

func (fr *Recovery) Run() {
	dir, err := os.ReadDir("wal")
	if err != nil {
		return
	}

	buckets := make(map[string]*persistence.Bucket)
	files := make([]string, 0)

	// TODO: Can process each file in parallel, merge into a single counter later
	for _, entry := range dir {
		if entry.IsDir() {
			continue
		}

		f, err := os.Open("wal/" + entry.Name())
		if err != nil {
			continue
		}

		files = append(files, f.Name())
		reader := bufio.NewReader(f)
		for {
			line, err := reader.ReadString('\n')
			if err == io.EOF {
				break
			}
			if err != nil {
				continue
			}

			parts := strings.Split(strings.TrimSpace(line), "|")
			bucketId := parts[0]
			counter, err := strconv.Atoi(parts[1])
			if err != nil {
				return
			}
			ts, err := strconv.Atoi(parts[1])
			if err != nil {
				return
			}

			buckets[bucketId] = &persistence.Bucket{
				BucketId:  bucketId,
				Counter:   uint64(counter),
				Timestamp: time.UnixMilli(int64(ts)),
				ETag:      "",
			}
		}

		err = f.Close()
		if err != nil {
			fr.logger.Printf("[RECOVERY] Failed to close file Filename: %s, Error: %s", f.Name(), err)
		}
	}

	total := len(buckets)
	pass := 0
	fail := 0
	skipped := 0
	for bucketId, bucket := range buckets {
		bkt, err := fr.p.GetBucket(context.Background(), bucketId)
		switch err {
		case nil:
			if bucket.Timestamp.After(bkt.Timestamp) {
				_, err := fr.p.UpsertBucket(context.Background(), bucketId, bucket, nil)
				if err != nil {
					fr.logger.Printf("[RECOVERY] Failed to update bucket BucketId: %s, Counter: %d, Error: %s", bucketId, bucket.Counter, err)
					fail++
				} else {
					fr.logger.Printf("[RECOVERY] Updated bucket BucketId: %s, Counter: %d", bucketId, bucket.Counter)
					pass++
				}
			} else {
				fr.logger.Printf("[RECOVERY] Bucket has a newer record. Skipping... BucketId: %s, CacheTs: %s, DbTs: %s", bucketId, bucket.Timestamp.String(), bkt.Timestamp.String())
				skipped++
			}
		case persistence.ErrBucketNotFound:
			_, err := fr.p.UpsertBucket(context.Background(), bucketId, bucket, nil)
			if err != nil {
				fr.logger.Printf("[RECOVERY] Failed to create bucket BucketId: %s, Counter: %d, Error: %s", bucketId, bucket.Counter, err)
				fail++
			} else {
				fr.logger.Printf("[RECOVERY] Created bucket BucketId: %s, Counter: %d", bucketId, bucket.Counter)
				pass++
			}
		default:
			fr.logger.Printf("[RECOVERY] Created bucket BucketId: %s, Counter: %d, Error: %s", bucketId, bucket.Counter, err)
			fail++
		}
	}

	fr.logger.Printf("[RECOVERY] Completed Total: %d, Pass: %d, Fail: %d, Skipped: %d", total, pass, fail, skipped)

	for _, file := range files {
		err := os.Remove(file)
		if err != nil {
			fr.logger.Printf("[RECOVERY] Failed to remove file File: %s, Error: %s", file, err)
		}
	}
}
