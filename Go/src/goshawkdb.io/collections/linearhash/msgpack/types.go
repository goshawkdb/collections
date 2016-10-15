package msgpack

//go:generate msgp

//msgp:ignore Root

import (
	"github.com/tinylib/msgp/msgp"
)

func NewRoot(hashKey []byte) *Root {
	return &Root{
		raw:         new(RootRaw),
		Size:        0,
		BucketCount: 2,
		SplitIndex:  0,
		MaskHigh:    3,
		MaskLow:     1,
		HashKey:     hashKey,
	}
}

type Root struct {
	raw         *RootRaw
	Size        int64
	BucketCount int64
	SplitIndex  uint64
	MaskHigh    uint64
	MaskLow     uint64
	HashKey     []byte
}

func (r *Root) UpdateRaw() *RootRaw {
	raw := r.raw
	raw.Size.AsInt(r.Size)
	raw.BucketCount.AsInt(r.BucketCount)
	raw.SplitIndex.AsUint(r.SplitIndex)
	raw.MaskHigh.AsUint(r.MaskHigh)
	raw.MaskLow.AsUint(r.MaskLow)
	raw.HashKey = r.HashKey
	return raw
}

type RootRaw struct {
	Size        msgp.Number
	BucketCount msgp.Number
	SplitIndex  msgp.Number
	MaskHigh    msgp.Number
	MaskLow     msgp.Number
	HashKey     []byte
}

func (rr *RootRaw) ToRoot() *Root {
	size, wasInt := rr.Size.Int()
	if !wasInt {
		sizeU, _ := rr.Size.Uint()
		size = int64(sizeU)
	}

	bc, wasInt := rr.BucketCount.Int()
	if !wasInt {
		bcU, _ := rr.BucketCount.Uint()
		bc = int64(bcU)
	}

	siU, wasUint := rr.SplitIndex.Uint()
	if !wasUint {
		si, _ := rr.SplitIndex.Int()
		siU = uint64(si)
	}

	mhU, wasUint := rr.MaskHigh.Uint()
	if !wasUint {
		mh, _ := rr.MaskHigh.Int()
		mhU = uint64(mh)
	}

	mlU, wasUint := rr.MaskLow.Uint()
	if !wasUint {
		ml, _ := rr.MaskLow.Int()
		mlU = uint64(ml)
	}

	return &Root{
		raw:         rr,
		Size:        size,
		BucketCount: bc,
		SplitIndex:  siU,
		MaskHigh:    mhU,
		MaskLow:     mlU,
		HashKey:     rr.HashKey,
	}
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
