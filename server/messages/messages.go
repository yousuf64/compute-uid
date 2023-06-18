package messages

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
)

type UpdateCounterMessage struct {
	BucketId string
	Counter  uint64
}

type UpdateETagMessage struct {
	BucketId string
	ETag     azcore.ETag
}

type DumpETagMessage struct {
	BucketId string
}

type FlushQueueMessage struct {
	QueueMapId string
	Filename   string
	Data       map[string]*BucketData
}

type BucketData struct {
	Counter uint64
	ETag    azcore.ETag
}
