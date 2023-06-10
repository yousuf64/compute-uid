package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/google/uuid"
	"github.com/hashicorp/golang-lru/v2"
	"github.com/yousuf64/shift"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"
)

const HeaderBucketId = "x-bucket-id"

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

type ErrorResponse struct {
	Status       int    `json:"-"`
	ErrorMessage string `json:"error"`
}

func NewErrorResponse(status int, message string) ErrorResponse {
	return ErrorResponse{status, message}
}

func (e ErrorResponse) Error() string {
	return e.ErrorMessage
}

func ErrorHandler(next shift.HandlerFunc) shift.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request, route shift.Route) error {
		err := next(w, r, route)
		if err != nil {
			errResp, ok := err.(ErrorResponse)
			if !ok {
				errResp.Status = http.StatusInternalServerError
				errResp.ErrorMessage = err.Error()
			}

			w.Header().Add("Content-Type", "application/json")
			w.WriteHeader(errResp.Status)
			if encodeErr := json.NewEncoder(w).Encode(err); encodeErr != nil {
				_, _ = w.Write([]byte("error encoding failed"))
			}
		}
		return nil
	}
}

func main() {
	port := flag.Int("port", 0, "server http port")
	flag.Parse()

	if *port == 0 {
		log.Fatal("specify --port flag")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	idleConnsClosed := make(chan struct{})

	cache, err := lru.New[string, *Metadata](128)
	if err != nil {
		log.Fatal(cache)
	}

	client := connect()
	countersContainer, err := client.NewContainer("unique-id", "counters")

	r := shift.New()
	r.Use(ErrorHandler)
	r.GET("/generate-uid/:bucketId", func(w http.ResponseWriter, r *http.Request, route shift.Route) error {
		headerBuckerId := r.Header.Get(HeaderBucketId)
		if headerBuckerId == "" {
			return NewErrorResponse(http.StatusBadRequest, fmt.Sprintf("%s expected in the header", HeaderBucketId))
		}

		bucketId := route.Params.Get("bucketId")
		if headerBuckerId != bucketId {
			return NewErrorResponse(http.StatusBadRequest, fmt.Sprintf("%s should match the :bucketId in the route", HeaderBucketId))
		}

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

		if md.Counter >= 999999 {
			return ErrorResponse{http.StatusConflict, "that's it for today :|"}
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
		log.Printf("replying %+v", resp)
		return Reply200(w, resp)
	})

	log.Printf("listening on port: %d", *port)
	server := http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: r.Serve(),
	}

	go func() {
		<-ctx.Done()

		log.Println("starting graceful shutdown")
		if err := server.Shutdown(context.Background()); err != nil {
			log.Printf("graceful shutdown failed: %v\n", err)
			return
		}

		log.Println("closed all listeners and connections")
		close(idleConnsClosed)
	}()

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}

	<-idleConnsClosed
}

func Reply200(w http.ResponseWriter, response any) error {
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	err := json.NewEncoder(w).Encode(response)
	if err != nil {
		log.Print("failed to encode the response")
		return err
	}
	return nil
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
