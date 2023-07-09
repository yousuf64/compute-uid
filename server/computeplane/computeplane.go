package computeplane

import (
	"fmt"
	"github.com/yousuf64/compute-uid/server/channels"
	"github.com/yousuf64/compute-uid/server/compute"
	"github.com/yousuf64/compute-uid/server/persistence"
	"hash/maphash"
	"log"
)

type ComputePlane struct {
	computes []*compute.Compute
	hashIdx  func(bucketId string) int
	logger   *log.Logger
}

func New(size int, persistence *persistence.Persistence, logger *log.Logger) *ComputePlane {
	computes := make([]*compute.Compute, size)
	for i := 0; i < size; i++ {
		computes[i] = compute.New(fmt.Sprintf("c%d", i), persistence, logger)
	}

	seed := maphash.MakeSeed()
	vsp := &ComputePlane{
		computes: computes,
		logger:   logger,
		hashIdx: func(bucketId string) int {
			hash := maphash.String(seed, bucketId)
			idx := hash % uint64(size)
			return int(idx)
		},
	}
	go vsp.listenInvalidateBucket()
	return vsp
}

func (cp *ComputePlane) ComputeId(bucketId string) (uint64, error) {
	idx := cp.hashIdx(bucketId)
	return cp.computes[idx].ComputeId(bucketId)
}

func (cp *ComputePlane) listenInvalidateBucket() {
	for msg := range channels.InvalidateBucket {
		cp.logger.Printf("[ComputePlane] Received InvalidateBucketMessage BucketId: %s\n", msg.BucketId)
		idx := cp.hashIdx(msg.BucketId)
		c := cp.computes[idx]
		cp.logger.Printf("[ComputePlane] Delegating InvalidateBucketMessage ComputeId: %s BucketId: %s\n", c.Id(), msg.BucketId)
		go cp.computes[idx].InvalidateBucket(msg)
	}
}
