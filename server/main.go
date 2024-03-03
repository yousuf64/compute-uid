package main

import (
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/yousuf64/compute-uid/server/computeplane"
	"github.com/yousuf64/compute-uid/server/flusher"
	"github.com/yousuf64/compute-uid/server/persistence"
	"github.com/yousuf64/compute-uid/server/provisioner"
	"github.com/yousuf64/compute-uid/server/queuemapplane"
	"github.com/yousuf64/compute-uid/server/recovery"
	"github.com/yousuf64/compute-uid/server/server"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
)

func main() {
	appPort := 80
	cosmosAddr := "https://127.0.0.1:8081"
	cosmosDatabaseId := "compute-uid"
	cosmosContainerId := "counters"
	walPath := "wal"

	var err error
	if portStr, ok := os.LookupEnv("APP_PORT"); ok {
		appPort, err = strconv.Atoi(portStr)
		if err != nil {
			log.Fatal(err)
		}
	}

	if cosmosAddrStr, ok := os.LookupEnv("COSMOS_ADDR"); ok {
		cosmosAddr = cosmosAddrStr
	}

	if walPathStr, ok := os.LookupEnv("WAL_PATH"); ok {
		walPath = walPathStr
	}

	if cosmosDatabaseIdStr, ok := os.LookupEnv("COSMOS_DATABASE_ID"); ok {
		cosmosDatabaseId = cosmosDatabaseIdStr
	}

	if cosmosContainerIdStr, ok := os.LookupEnv("COSMOS_CONTAINER_ID"); ok {
		cosmosContainerId = cosmosContainerIdStr
	}

	log.Println("APP_PORT =", appPort)
	log.Println("COSMOS_ADDR =", cosmosAddr)
	log.Println("COSMOS_DATABASE_ID =", cosmosDatabaseId)
	log.Println("COSMOS_CONTAINER_ID =", cosmosContainerId)
	log.Println("WAL_PATH =", walPath)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	idleConnsClosed := make(chan struct{})

	logger := log.New(os.Stdout, "", 0)

	client := CosmosClient(cosmosAddr)
	err = provisioner.Provision(ctx, client, azcosmos.DatabaseProperties{ID: cosmosDatabaseId}, azcosmos.ContainerProperties{
		ID: cosmosContainerId,
		PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
			Paths:   []string{"/id"},
			Version: 1,
		},
		IndexingPolicy: &azcosmos.IndexingPolicy{
			Automatic:    false,
			IndexingMode: azcosmos.IndexingModeNone,
		},
	})
	if err != nil {
		log.Fatalf("Failed to provision the database %s Error: %s", cosmosDatabaseId, err)
	}

	countersContainer, err := client.NewContainer(cosmosDatabaseId, cosmosContainerId)
	prs := persistence.New(countersContainer, logger)

	rcv := recovery.New(prs, logger)
	rcv.Run()

	cp := computeplane.New(2, prs, logger)

	queuemapplane.Listen(2, walPath, logger)
	flusher.Listen(prs, logger)

	srv := http.Server{
		Addr:    fmt.Sprintf(":%d", appPort),
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

	log.Printf("listening on port: %d", appPort)
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
