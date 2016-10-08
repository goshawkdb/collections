package msgpack

//go:generate msgp

type Root struct {
	Size        int64
	BucketCount int64
	SplitIndex  uint64
	MaskHigh    uint64
	MaskLow     uint64
	Hashkey     []byte
}

type Bucket [][]byte

const (
	BucketCapacity    = 64
	UtilizationFactor = 0.75
)

func (r *Root) BucketIndex(key uint64) uint64 {
	if hl := key & r.MaskLow; hl >= r.SplitIndex {
		return hl
	} else {
		return key & r.MaskHigh
	}
}

func (r *Root) NeedsSplit() bool {
	return (float64(r.Size) / float64(BucketCapacity*r.BucketCount)) > UtilizationFactor
}
