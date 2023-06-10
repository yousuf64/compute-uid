package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/google/uuid"
	"github.com/hashicorp/golang-lru/v2"
	"github.com/yousuf64/shift"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type Metadata struct {
	Counter   uint64
	Mu        sync.Mutex
	Timestamp time.Time
}

type CounterItem struct {
	Id        string `json:"id"`
	Counter   uint64 `json:"counter"`
	Timestamp uint64 `json:"_ts"`
}

type Response struct {
	Uid uint64 `json:"uid"`
}

func main() {
	cache, err := lru.New[string, *Metadata](128)
	if err != nil {
		log.Fatal(cache)
	}

	client := connect()
	countersContainer, err := client.NewContainer("unique-id", "counters")

	r := shift.New()
	r.GET("/generate-uid/:bucketId", func(w http.ResponseWriter, r *http.Request, route shift.Route) error {
		bucketId := route.Params.Get("bucketId")
		_, err := uuid.Parse(bucketId)
		if err != nil {
			log.Printf("failed to parse bucketId: %s\n", bucketId)
			return err
		}

		md, ok := cache.Get(bucketId)
		if !ok {
			log.Printf("cache miss for bucketId: %s\n", bucketId)
			// Read from cosmos.
			md = &Metadata{
				Counter:   0,
				Mu:        sync.Mutex{},
				Timestamp: time.Now(),
			}

			log.Printf("writing empty metadata on cache for bucketId: %s\n", bucketId)
			if peekMd, found, _ := cache.PeekOrAdd(bucketId, md); found {
				log.Printf("found entry on cache peek for bucketId: %s\n", bucketId)
				md = peekMd
				md.Mu.Lock() // Pauses racing goroutines.
				// Handle scenario where md is removed from cache.
			} else {
				// Newly written.
				log.Printf("found newly created entry on cache peek for bucketId: %s\n", bucketId)
				md.Mu.Lock() // Pauses racing goroutines.

				pk := azcosmos.NewPartitionKeyString(bucketId)
				log.Printf("reading item from cosmos partitionKey: %s, id: %s\n", bucketId, bucketId)
				response, err := countersContainer.ReadItem(context.Background(), pk, bucketId, nil)
				if err != nil {
					var responseErr *azcore.ResponseError
					errors.As(err, &responseErr)
					if responseErr == nil || responseErr.StatusCode != http.StatusNotFound {
						cache.Remove(bucketId)
						md.Mu.Unlock()
						log.Printf("failed to read item from cosmos for partitionKey: %s, id: %s, err: %v\n", bucketId, bucketId, err)
						return err
					}
					log.Printf("item not found in cosmos for partitionKey: %s, id: %s\n", bucketId, bucketId)
				} else {
					var counterItem *CounterItem
					err = json.NewDecoder(response.RawResponse.Body).Decode(&counterItem)
					if err != nil {
						cache.Remove(bucketId)
						md.Mu.Unlock()
						log.Printf("failed to decode CounterItem from cosmos for partitionKey: %s, id: %s\n", bucketId, bucketId)
						return err
					}
					log.Printf("decoded CounterItem %+v for partitionKey: %s, id: %s\n", *counterItem, bucketId, bucketId)
					md.Counter = counterItem.Counter
					md.Timestamp = time.Unix(int64(counterItem.Timestamp), 0)
				}
			}
		} else {
			log.Printf("cache pass for bucketId %s\n", bucketId)
			md.Mu.Lock()
		}

		if ok := isSameDate(md.Timestamp, time.Now()); ok {
			md.Counter++
			log.Printf("incremented counter for bucketId: %s\n", bucketId)
		} else {
			md.Timestamp = time.Now()
			md.Counter = 1
			log.Printf("reset counter for bucketId: %s\n", bucketId)
		}
		go func() {
			log.Printf("preparing to flush counter for bucketId: %s\n", bucketId)
			counterItem := CounterItem{
				Id:      bucketId,
				Counter: md.Counter,
			}
			b, err := json.Marshal(counterItem)
			if err != nil {
				log.Println("[FLUSH] failed to marshal in goroutine")
				return
			}
			pk := azcosmos.NewPartitionKeyString(bucketId)
			_, err = countersContainer.UpsertItem(context.Background(), pk, b, nil)
			if err != nil {
				log.Printf("[FLUSH] failed to upsert item for partitionKey: %v, id: %s\n", pk, counterItem.Id)
				return
			}
			log.Printf("[FLUSH] successfully flushed partitionKey: %v, id: %s\n", pk, counterItem.Id)
		}()
		counter := md.Counter
		md.Mu.Unlock()

		yyyy, mm, dd := md.Timestamp.Date()
		uid, err := strconv.Atoi(fmt.Sprintf("%d%02d%02d%06d", yyyy, mm, dd, counter))
		if err != nil {
			return err
		}

		resp := Response{Uid: uint64(uid)}
		err = json.NewEncoder(w).Encode(resp)
		if err != nil {
			log.Printf("error encoding response for bucketId: %s\n", bucketId)
			return err
		}
		log.Printf("wrote %d for bucketId: %s\n", resp.Uid, bucketId)
		return nil
	})
	if err := http.ListenAndServe(":3000", r.Serve()); err != nil {
		log.Fatal(err)
	}
}

func isSameDate(t1, t2 time.Time) bool {
	return t1.UTC().Truncate(time.Hour * 24).Equal(t2.UTC().Truncate(time.Hour * 24))
}

func connect() *azcosmos.Client {
	cred, err := azcosmos.NewKeyCredential("C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==")
	if err != nil {
		log.Fatal(err)
	}

	client, err := azcosmos.NewClientWithKey("https://localhost:8081", cred, nil)
	if err != nil {
		log.Fatal(err)
	}

	return client
}

// TODO: Handle concurrency scenarios on LRUCache overflow.
// TODO: Handle concurrency scenarios on cache remove (on errors).
