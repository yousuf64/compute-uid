package provisioner

import (
	"context"
	"errors"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"log"
	"net/http"
)

func Provision(ctx context.Context, client *azcosmos.Client, dbProps azcosmos.DatabaseProperties, containerProps azcosmos.ContainerProperties) error {
	_, err := client.CreateDatabase(ctx, dbProps, nil)
	if err != nil {
		var responseErr *azcore.ResponseError
		errors.As(err, &responseErr)
		if responseErr != nil && responseErr.StatusCode == http.StatusConflict {
			log.Printf("Database %s provisioned already", dbProps.ID)
		} else {
			return responseErr
		}
	}

	database, err := client.NewDatabase(dbProps.ID)
	if err != nil {
		return err
	}

	_, err = database.CreateContainer(ctx, containerProps, nil)
	if err != nil {
		var responseErr *azcore.ResponseError
		errors.As(err, &responseErr)
		if responseErr.StatusCode == http.StatusConflict {
			log.Printf("Container %s provisioned already", containerProps.ID)
		} else {
			return responseErr
		}
	}
	return nil
}
