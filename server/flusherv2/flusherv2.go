package flusherv2

import (
	"log"
	"sync"
	"unique-id-generator/server/channels"
)

func Listen(logger *log.Logger) {
	(&sync.Once{}).Do(func() {
		go func() {
			for m := range channels.FlushQueue {
				logger.Printf("[Flusher] Received FlushQueueMessage %+v", m)
			}
		}()
	})
}
