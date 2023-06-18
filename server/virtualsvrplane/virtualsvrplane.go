package virtualsvrplane

import (
	"hash/maphash"
	"log"
	"unique-id-generator/server/persistence"
	"unique-id-generator/server/virtualsvr"
)

type VirtualSvrPlane struct {
	svrs    []*virtualsvr.VirtualSvr
	hashIdx func(bucketId string) int
}

func New(size int, persistence *persistence.Persistence, logger *log.Logger) *VirtualSvrPlane {
	svrs := make([]*virtualsvr.VirtualSvr, size)
	for i := 0; i < size; i++ {
		svrs[i] = virtualsvr.New(persistence, logger)
	}

	seed := maphash.MakeSeed()
	return &VirtualSvrPlane{
		svrs: svrs,
		hashIdx: func(bucketId string) int {
			hash := maphash.String(seed, bucketId)
			idx := hash % uint64(size)
			return int(idx)
		},
	}
}

func (vsp *VirtualSvrPlane) ComputeId(bucketId string) (uint64, error) {
	idx := vsp.hashIdx(bucketId)
	return vsp.svrs[idx].Serve(bucketId)
}

func (vsp *VirtualSvrPlane) Invalidate(bucketId string) {

}
