package queuemap

import (
	"errors"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/google/uuid"
	"github.com/yousuf64/compute-uid/server/channels"
	"github.com/yousuf64/compute-uid/server/messages"
	"log"
	"math/rand"
	"os"
	"time"
)

type counterData struct {
	value uint64
	ts    time.Time
}

type QueueMap struct {
	id          string
	walPath     string
	logger      *log.Logger
	counterChan <-chan *messages.UpdateCounterMessage
	etagChan    <-chan *messages.UpdateETagMessage
	flushChan   chan struct{}
	counters    map[string]*counterData
	etags       map[string]azcore.ETag
	logFile     *os.File
}

func New(id string, walPath string, logger *log.Logger) (*QueueMap, chan<- *messages.UpdateCounterMessage, chan<- *messages.UpdateETagMessage) {
	counterChan := make(chan *messages.UpdateCounterMessage)
	etagChan := make(chan *messages.UpdateETagMessage)
	ensureDir(walPath)

	qm := &QueueMap{
		id:          id,
		walPath:     walPath,
		logger:      logger,
		counterChan: counterChan,
		etagChan:    etagChan,
		flushChan:   make(chan struct{}),
		counters:    make(map[string]*counterData),
		etags:       make(map[string]azcore.ETag),
		logFile:     newLogFile(walPath, id),
	}
	go qm.listen()
	go qm.timer()
	return qm, counterChan, etagChan
}

func (qm *QueueMap) listen() {
	for {
		select {
		case m, stop := <-qm.counterChan:
			if !stop {
				return
			}

			qm.logger.Printf("[QueueMap %s] Received message BucketId: %s, Counter: %v\n", qm.id, m.BucketId, m.Counter)
			d, ok := qm.counters[m.BucketId]
			if !ok {
				d = &counterData{}
				qm.counters[m.BucketId] = d
			}
			d.value = m.Counter
			d.ts = m.Timestamp

			qm.logToDisk(m)

			if len(qm.counters) >= 5 {
				qm.logger.Printf("[QueueMap %s] Exceeded threshold. Sending flush signal...\n", qm.id)
				qm.flushChan <- struct{}{}
			}
		case m, stop := <-qm.etagChan:
			if !stop {
				return
			}

			qm.logger.Printf("[QueueMap %s] Received UpdateETagMessage BucketId: %s, ETag: %s\n", qm.id, m.BucketId, m.ETag)
			qm.etags[m.BucketId] = m.ETag
		case <-qm.flushChan:
			qm.logger.Printf("[QueueMap %s] Received flush signal\n", qm.id)
			if len(qm.counters) == 0 {
				qm.logger.Printf("[QueueMap %s] Zero entries in the queue map. Aborting flush queue message...\n", qm.id)
				break
			}

			m := make(map[string]*messages.BucketData, len(qm.counters))
			for k, v := range qm.counters {
				m[k] = &messages.BucketData{
					Counter:   v.value,
					ETag:      qm.etags[k],
					Timestamp: v.ts,
				}
			}

			filename := qm.logFile.Name()
			qm.reset()

			qm.logger.Printf("[QueueMap %s] Sending flush queue message...\n", qm.id)
			go func() {
				channels.FlushQueue <- &messages.FlushQueueMessage{
					QueueMapId: qm.id,
					Filename:   filename,
					Data:       m,
				}
			}()
		}
	}
}

func (qm *QueueMap) timer() {
	t := time.NewTimer(time.Second * time.Duration(randInRange(60, 120)))
	for range t.C {
		qm.logger.Printf("[QueueMap %s] Timer timed out. Sending flush signal...\n", qm.id)
		qm.flushChan <- struct{}{}
		t.Reset(time.Second * time.Duration(randInRange(60, 120)))
	}
}

func (qm *QueueMap) logToDisk(m *messages.UpdateCounterMessage) {
	_, err := qm.logFile.Write([]byte(fmt.Sprintf("%s|%d|%d\n", m.BucketId, m.Counter, m.Timestamp.UnixMilli())))
	if err != nil {
		qm.logger.Printf("[QueueMap %s] Failed to log to disk File: %s, BucketId: %s, Counter: %v, Error: %s\n", qm.id, qm.logFile.Name(), m.BucketId, m.Counter, err)
		return
	}
	qm.logger.Printf("[QueueMap %s] Logged to disk File: %s, BucketId: %s, Counter: %v\n", qm.id, qm.logFile.Name(), m.BucketId, m.Counter)
}

func (qm *QueueMap) reset() {
	err := qm.logFile.Close()
	if err != nil {
		qm.logger.Printf("[QueueMap %s] Failed to close log file Error: %s\n", qm.id, err)
	}
	qm.logFile = newLogFile(qm.walPath, qm.id)
	qm.counters = make(map[string]*counterData)
}

func newLogFile(path string, id string) *os.File {
	fid, err := uuid.NewUUID()
	fname := fmt.Sprintf("%s/%s_%s", path, id, fid.String())
	file, err := os.OpenFile(fname, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}
	return file
}

func ensureDir(path string) {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		err := os.Mkdir(path, os.ModePerm)
		if err != nil {
			log.Println(err)
		}
	}
}

func randInRange(min, max int) int {
	source := rand.NewSource(time.Now().UnixMicro())
	n := rand.New(source).Intn(max-min) + min
	return n
}
