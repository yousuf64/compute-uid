package main

import (
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"log"
)

func CosmosClient() *azcosmos.Client {
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
