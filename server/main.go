package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"unique-id-generator/server/computeplane"
	"unique-id-generator/server/flusher"
	"unique-id-generator/server/flusherrecovery"
	"unique-id-generator/server/persistence"
	"unique-id-generator/server/queuemapplane"
	"unique-id-generator/server/server"
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

	client := CosmosClient()
	countersContainer, err := client.NewContainer("unique-id", "counters")
	prs := persistence.New(countersContainer, logger)

	frec := flusherrecovery.New(prs, logger)
	frec.Recover()

	cp := computeplane.New(2, prs, logger)

	queuemapplane.Listen(2, logger)
	flusher.Listen(prs, logger)

	srv := http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: server.New(cp, logger),
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
// TODO: Handle stale cached counter
// TODO: Another node could have a stale counter for the bucket (who may again end up being the handler), handle it
