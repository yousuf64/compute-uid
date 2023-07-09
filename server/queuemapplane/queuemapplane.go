package queuemapplane

import (
	"fmt"
	"github.com/yousuf64/compute-uid/server/channels"
	"github.com/yousuf64/compute-uid/server/messages"
	"github.com/yousuf64/compute-uid/server/queuemap"
	"hash/maphash"
	"log"
	"sync"
)

type chanTuple struct {
	QueueChan chan<- *messages.UpdateCounterMessage
	ETagChan  chan<- *messages.UpdateETagMessage
}

type queueMapPlane struct {
	hashSeed maphash.Seed
	mapids   []string
	maps     []*chanTuple
	logger   *log.Logger
}

// Listen starts listening on streams.UpdateCounterMessage.
// Forwards the message to the relevant queuemap.QueueMap channel by hashing the bucket id.
func Listen(size int, logger *log.Logger) {
	(&sync.Once{}).Do(func() {
		mapids := make([]string, size)
		maps := make([]*chanTuple, size)

		for i := 0; i < size; i++ {
			id := fmt.Sprintf("qmap%d", i)
			_, qmChan, etagChan := queuemap.New(id, logger)
			mapids[i] = id
			maps[i] = &chanTuple{
				QueueChan: qmChan,
				ETagChan:  etagChan,
			}
		}

		qmp := &queueMapPlane{
			hashSeed: maphash.MakeSeed(),
			mapids:   mapids,
			maps:     maps,
			logger:   logger,
		}

		go qmp.listener()
		go qmp.etagListener()
	})
}

func (qmp *queueMapPlane) listener() {
	for msg := range channels.UpdateCounter {
		qmp.logger.Printf("[QueueMapPlane] Received UpdateCounterMessage BucketId: %s, Counter: %v\n", msg.BucketId, msg.Counter)
		idx := qmp.hashIdx(msg.BucketId)
		qmp.logger.Printf("[QueueMapPlane] Delegating UpdateCounterMessage to QM: %s, BucketId: %s, Counter: %v\n", qmp.mapids[idx], msg.BucketId, msg.Counter)
		qmp.maps[idx].QueueChan <- msg
	}
}

func (qmp *queueMapPlane) etagListener() {
	for msg := range channels.UpdateETag {
		qmp.logger.Printf("[QueueMapPlane] Received UpdateETagMessage BucketId: %s, ETag: %s\n", msg.BucketId, msg.ETag)
		idx := qmp.hashIdx(msg.BucketId)
		qmp.logger.Printf("[QueueMapPlane] Delegating UpdateETagMessage to QM: %s, BucketId: %s, Counter: %s\n", qmp.mapids[idx], msg.BucketId, msg.ETag)
		qmp.maps[idx].ETagChan <- msg
	}
}

func (qmp *queueMapPlane) hashIdx(bucketId string) uint64 {
	hash := maphash.String(qmp.hashSeed, bucketId)
	idx := hash % uint64(len(qmp.maps))
	return idx
}
