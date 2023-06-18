package flusherrecovery

import (
	"bufio"
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"unique-id-generator/server/persistence"
)

type FlusherRecovery struct {
	p      *persistence.Persistence
	logger *log.Logger
}

func New(p *persistence.Persistence, logger *log.Logger) *FlusherRecovery {
	return &FlusherRecovery{
		p:      p,
		logger: logger,
	}
}

func (fr *FlusherRecovery) Recover() {
	dir, err := os.ReadDir("flusherlogs")
	if err != nil {
		return
	}

	counters := make(map[string]*persistence.Counter)
	files := make([]string, 0)

	// TODO: Can process each file in parallel, merge into a single counter later
	for _, entry := range dir {
		if entry.IsDir() {
			continue
		}

		f, err := os.Open("flusherlogs/" + entry.Name())
		if err != nil {
			continue
		}

		files = append(files, f.Name())
		reader := bufio.NewReader(f)
		for {
			line, err := reader.ReadString('\n')
			if err == io.EOF {
				break
			}
			if err != nil {
				continue
			}

			if line == "" {
				// EOF
				break
			}

			parts := strings.Split(strings.TrimSpace(line), "|")
			bucketId := parts[0]
			counter, err := strconv.Atoi(parts[1])
			if err != nil {
				return
			}
			etag := azcore.ETag(parts[2])

			counters[bucketId] = &persistence.Counter{
				BucketId: bucketId,
				Counter:  uint64(counter),
				ETag:     etag,
			}
		}

		f.Close()
	}

	for bucketId, counter := range counters {
		_, err = fr.p.SaveCounter(context.Background(), bucketId, counter, &azcosmos.ItemOptions{IfMatchEtag: &counter.ETag})
		if err != nil {
			fr.logger.Printf("failed to save this file")
		}
	}

	for _, file := range files {
		err := os.Remove(file)
		if err != nil {
			fr.logger.Printf("failed to remove file %s %s\n", file, err)
		}
	}
}
