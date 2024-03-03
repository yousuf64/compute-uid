package main

import (
	"context"
	"crypto/tls"
	"errors"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"log"
	"net/http"
)

func CosmosClient(addr string) *azcosmos.Client {
	http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}

	cred, err := azcosmos.NewKeyCredential("C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==")
	if err != nil {
		log.Fatal(err)
	}

	client, err := azcosmos.NewClientWithKey(addr, cred, &azcosmos.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Transport: &http.Client{
				Transport: http.DefaultTransport,
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	return client
}

func ProvisionDatabase(ctx context.Context, client *azcosmos.Client, properties azcosmos.DatabaseProperties) error {
	_, err := client.CreateDatabase(ctx, properties, nil)
	if err != nil {
		var responseErr *azcore.ResponseError
		errors.As(err, &responseErr)
		if responseErr.StatusCode != http.StatusConflict {
			log.Printf("Database %s provisioned already", properties.ID)
			return responseErr
		}
	}
	return nil
}
