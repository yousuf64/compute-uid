package uidgen

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/hashicorp/golang-lru/v2"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

var ErrMaxLimitReached = errors.New("reached maximum for the day")

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

type UIDGen struct {
	cache     *lru.Cache[string, *Metadata]
	container *azcosmos.ContainerClient
	logger    *log.Logger
}

func New(cache *lru.Cache[string, *Metadata], container *azcosmos.ContainerClient, logger *log.Logger) *UIDGen {
	return &UIDGen{cache, container, logger}
}

func (u *UIDGen) Generate(bucketId string) (uint64, error) {
	md, ok := u.cache.Get(bucketId)
	if !ok {
		u.logger.Printf("cache miss for bucketId: %s\n", bucketId)
		// Read from cosmos.
		md = &Metadata{
			Counter:   0,
			Mu:        sync.Mutex{},
			Timestamp: time.Now(),
		}

		u.logger.Printf("writing empty metadata on cache for bucketId: %s\n", bucketId)
		if peekMd, found, _ := u.cache.PeekOrAdd(bucketId, md); found {
			u.logger.Printf("found entry on cache peek for bucketId: %s\n", bucketId)
			md = peekMd
			md.Mu.Lock() // Pauses racing goroutines.
			// TODO: Handle scenario where md is removed from cache.
		} else {
			// Newly written.
			u.logger.Printf("found newly created entry on cache peek for bucketId: %s\n", bucketId)
			md.Mu.Lock() // Pauses racing goroutines.

			pk := azcosmos.NewPartitionKeyString(bucketId)
			u.logger.Printf("reading item from cosmos partitionKey: %s, id: %s\n", bucketId, bucketId)
			response, err := u.container.ReadItem(context.Background(), pk, bucketId, nil)
			if err != nil {
				var responseErr *azcore.ResponseError
				errors.As(err, &responseErr)
				if responseErr == nil || responseErr.StatusCode != http.StatusNotFound {
					u.cache.Remove(bucketId)
					md.Mu.Unlock()
					u.logger.Printf("failed to read item from cosmos for partitionKey: %s, id: %s, err: %v\n", bucketId, bucketId, err)
					return 0, err
				}
				u.logger.Printf("item not found in cosmos for partitionKey: %s, id: %s\n", bucketId, bucketId)
			} else {
				var counterItem *CounterItem
				err = json.NewDecoder(response.RawResponse.Body).Decode(&counterItem)
				if err != nil {
					u.cache.Remove(bucketId)
					md.Mu.Unlock()
					u.logger.Printf("failed to decode CounterItem from cosmos for partitionKey: %s, id: %s\n", bucketId, bucketId)
					return 0, err
				}
				u.logger.Printf("decoded CounterItem %+v for partitionKey: %s, id: %s\n", *counterItem, bucketId, bucketId)
				md.Counter = counterItem.Counter
				md.Timestamp = time.Unix(int64(counterItem.Timestamp), 0)
			}
		}
	} else {
		u.logger.Printf("cache pass for bucketId %s\n", bucketId)
		md.Mu.Lock()
	}

	if md.Counter >= 999999 {
		return 0, ErrMaxLimitReached
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
		_, err = u.container.UpsertItem(context.Background(), pk, b, nil)
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
		return 0, err
	}

	return uint64(uid), nil
}

func isSameDate(t1, t2 time.Time) bool {
	return t1.UTC().Truncate(time.Hour * 24).Equal(t2.UTC().Truncate(time.Hour * 24))
}
