package batcher

import (
	"log"
	"sync"
	"unique-id-generator/server/streams"
)

func Listen(logger *log.Logger) {
	(&sync.Once{}).Do(func() {
		go func() {
			for m := range streams.FlushQueueMessage {
				logger.Printf("[Batcher] Received flush queue message %+v", m)
			}
		}()
	})
}
