package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/hashicorp/golang-lru/v2"
	"log"
	"net/http"
	"os"
	"os/signal"
	"unique-id-generator/server/flusher"
	"unique-id-generator/server/flusherplane"
	"unique-id-generator/server/persistence"
	"unique-id-generator/server/server"
	"unique-id-generator/server/uidgen"
)

func main() {
	port := flag.Int("port", 0, "server http port")
	flag.Parse()

	if *port == 0 {
		log.Fatal("specify --port flag")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	idleConnsClosed := make(chan struct{})

	logger := log.New(os.Stdout, "", 0)

	cache, err := lru.New[string, *uidgen.Metadata](128)
	if err != nil {
		log.Fatal(cache)
	}

	client := CosmosClient()
	countersContainer, err := client.NewContainer("unique-id", "counters")
	prs := persistence.New(countersContainer, logger)
	invalidateBucketChan := make(chan flusher.InvalidateBucketMessage)
	updateETagChan := make(chan flusher.UpdateETagMessage)

	fp := flusherplane.New(2, prs, invalidateBucketChan, updateETagChan, logger)
	uidGen := uidgen.New(cache, prs, fp, invalidateBucketChan, updateETagChan, logger)

	srv := http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: server.New(uidGen, logger),
	}

	go func() {
		<-ctx.Done()

		log.Println("starting graceful shutdown")
		if err := srv.Shutdown(context.Background()); err != nil {
			log.Printf("graceful shutdown failed: %v\n", err)
			return
		}

		log.Println("closed all listeners and connections")
		close(idleConnsClosed)
	}()

	log.Printf("listening on port: %d", *port)
	if err = srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}

	<-idleConnsClosed
}

// TODO: Traceable context.
// TODO: Handle concurrency scenarios on LRUCache overflow.
// TODO: Handle concurrency scenarios on cache remove (on errors).
// TODO: Document TTL.
// TODO: Cancel update token.
