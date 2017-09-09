// An LHash is a map structure using the linear-hashing
// algorithm. This implementation uses the SipHash hashing algorithm,
// which is available on many platforms and languages, and uses
// msgpack as the serialization format for the LHash state, which
// again, is widely available.
//
// Multiple connections may interact with the same underlying LHash
// objects at the same time, as GoshawkDB ensures through the use of
// strong serialization that any dependent operations are safely
// ordered.
package linearhash

import (
	"bytes"
	"encoding/binary"
	// "fmt"
	hash "github.com/dchest/siphash"
	"goshawkdb.io/client"
	mp "goshawkdb.io/collections/linearhash/msgpack"
	"math/rand"
	"time"
)

type LHash struct {
	// The underlying Object in GoshawkDB which holds the root data for
	// the LHash.
	ObjRef client.RefCap
	root   *mp.Root
	value  []byte
	refs   []client.RefCap
	k0     uint64
	k1     uint64
}

// Create a brand new empty LHash. This creates a new GoshawkDB Object
// and initialises it for use as an LHash.
func NewEmptyLHash(txr client.Transactor) (*LHash, error) {
	res, err := txr.Transact(func(txn *client.Transaction) (interface{}, error) {
		if rootObjRef, err := txn.Create([]byte{}); err != nil || txn.RestartNeeded() {
			return nil, err

		} else {
			lh := LHashFromObj(rootObjRef)
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			key := make([]byte, 16)
			rng.Read(key)
			lh.root = mp.NewRoot(key)

			refs := make([]client.RefCap, lh.root.BucketCount)
			lh.refs = refs
			for idx := range refs {
				if objRef, err := txn.Create(nil); err != nil || txn.RestartNeeded() {
					return nil, err
				} else {
					refs[idx] = objRef
					if err := lh.newEmptyBucket(objRef).write(txn, true); err != nil || txn.RestartNeeded() {
						return nil, err
					}
				}
			}

			return lh, lh.write(txn)
		}
	})
	if err == nil {
		return res.(*LHash), nil
	} else {
		return nil, err
	}
}

// Create an LHash object from an existing given GoshawkDB Object. Use
// this to regain access to an existing LHash which has already been
// created. This function does not do any initialisation: it assumes
// the Object passed is already initialised for LHash.
func LHashFromObj(objRef client.RefCap) *LHash {
	return &LHash{
		ObjRef: objRef,
	}
}

func (lh *LHash) populate(txn *client.Transaction) error {
	if value, refs, err := txn.Read(lh.ObjRef); err != nil || txn.RestartNeeded() {
		return err
	} else {
		// fmt.Println("read ->", value)
		rootraw := new(mp.RootRaw)
		if _, err := rootraw.UnmarshalMsg(value); err != nil {
			return err
		}
		lh.root = rootraw.ToRoot()
		lh.value = value
		lh.refs = refs
		lh.k0 = binary.LittleEndian.Uint64(lh.root.HashKey[0:8])
		lh.k1 = binary.LittleEndian.Uint64(lh.root.HashKey[8:16])
		// fmt.Printf("read %#v, %v %v\n", lh.root, lh.k0, lh.k1)
		return nil
	}
}

func (lh *LHash) hash(key []byte) uint64 {
	return hash.Hash(lh.k0, lh.k1, key)
}

// Search the LHash for the given key. The key is hashed using the
// SipHash algorithm, and comparison between keys is done with
// bytes.Equal. If no matching key is found, a nil ObjectRef is
// returned.
func (lh *LHash) Find(txr client.Transactor, key []byte) (*client.RefCap, error) {
	res, err := txr.Transact(func(txn *client.Transaction) (interface{}, error) {
		if err := lh.populate(txn); err != nil || txn.RestartNeeded() {
			return nil, err
		} else if bucket, err := lh.newBucket(txn, lh.refs[lh.root.BucketIndex(lh.hash(key))]); err != nil || txn.RestartNeeded() {
			return nil, err
		} else {
			return bucket.find(txn, key)
		}
	})
	if err == nil {
		return res.(*client.RefCap), nil
	} else {
		return nil, err
	}
}

// Idempotently add the given key and value to the LHash. The key is
// hashed using the SipHash algorithm, and comparison between keys is
// done with bytes.Equal. If a matching key is found, the
// corresponding value is updated.
func (lh *LHash) Put(txr client.Transactor, key []byte, value client.RefCap) error {
	_, err := txr.Transact(func(txn *client.Transaction) (interface{}, error) {
		if err := lh.populate(txn); err != nil || txn.RestartNeeded() {
			return nil, err
		} else if bucket, err := lh.newBucket(txn, lh.refs[lh.root.BucketIndex(lh.hash(key))]); err != nil || txn.RestartNeeded() {
			return nil, err
		} else if _, added, chainDelta, err := bucket.put(txn, key, value); err != nil || txn.RestartNeeded() {
			return nil, err
		} else if added || chainDelta != 0 {
			if added {
				lh.root.Size++
			}
			lh.root.BucketCount += chainDelta
			if lh.root.NeedsSplit() {
				if err := lh.split(txn); err != nil || txn.RestartNeeded() {
					return nil, err
				}
			}
			return nil, lh.write(txn)
		}
		// fmt.Printf("(%v) Put %v, added:%v; chainDelta:%v\n", lh.root.Size, key, added, chainDelta)
		return nil, nil
	})
	return err
}

// Idempotently remove any matching entry from the LHash. The key is
// hashed using the SipHash algorithm, and comparison between keys is
// done with bytes.Equal.
func (lh *LHash) Remove(txr client.Transactor, key []byte) error {
	_, err := txr.Transact(func(txn *client.Transaction) (interface{}, error) {
		if err := lh.populate(txn); err != nil || txn.RestartNeeded() {
			return nil, err
		} else {
			idx := lh.root.BucketIndex(lh.hash(key))
			if bucket, err := lh.newBucket(txn, lh.refs[idx]); err != nil || txn.RestartNeeded() {
				return nil, err
			} else if bNew, removed, chainDelta, err := bucket.remove(txn, key); err != nil || txn.RestartNeeded() {
				return nil, err
			} else if removed || chainDelta != 0 {
				if bNew == nil { // must keep old bucket even though it's empty
					if err := bucket.write(txn, true); err != nil || txn.RestartNeeded() {
						return nil, err
					}
				} else if bNew != bucket {
					lh.refs[idx] = bNew.objRef
				}
				if removed {
					lh.root.Size--
				}
				lh.root.BucketCount += chainDelta
				return nil, lh.write(txn)
			}
			return nil, nil
		}
	})
	return err
}

// Iterate over the entries in the LHash. Iteration order is
// undefined. Also note that as usual, the transaction in which the
// iteration is occurring may need to restart one or more times in
// which case the callback may be invoked several times for the same
// entry. To detect this, call ForEach from within a transaction of
// your own. Iteration will stop as soon as the callback returns a
// non-nil error, which will also abort the transaction.
func (lh *LHash) ForEach(txr client.Transactor, f func([]byte, client.RefCap) error) error {
	_, err := txr.Transact(func(txn *client.Transaction) (interface{}, error) {
		if err := lh.populate(txn); err != nil || txn.RestartNeeded() {
			return nil, err
		}
		for _, objRef := range lh.refs {
			if bucket, err := lh.newBucket(txn, objRef); err != nil || txn.RestartNeeded() {
				return nil, err
			} else if err := bucket.forEach(txn, f); err != nil || txn.RestartNeeded() {
				return nil, err
			}
		}
		return nil, nil
	})
	return err
}

// Returns the number of entries in the LHash.
func (lh *LHash) Size(txr client.Transactor) (int64, error) {
	res, err := txr.Transact(func(txn *client.Transaction) (interface{}, error) {
		if err := lh.populate(txn); err != nil || txn.RestartNeeded() {
			return nil, err
		}
		return lh.root.Size, nil
	})
	if err == nil {
		return res.(int64), nil
	} else {
		return -1, err
	}
}

func (lh *LHash) split(txn *client.Transaction) error {
	sOld := lh.root.SplitIndex
	if b, err := lh.newBucket(txn, lh.refs[sOld]); err != nil || txn.RestartNeeded() {
		return err
	} else if objRef, err := txn.Create(nil); err != nil || txn.RestartNeeded() {
		return err
	} else {
		bNew := lh.newEmptyBucket(objRef)
		lh.refs = append(lh.refs, bNew.objRef)

		lh.root.BucketCount++
		lh.root.SplitIndex++
		if 2*lh.root.SplitIndex == uint64(len(lh.refs)) {
			// we've split everything
			lh.root.SplitIndex = 0
			lh.root.MaskLow = lh.root.MaskHigh
			lh.root.MaskHigh = lh.root.MaskHigh*2 + 1
		}

		var bPrev, bNext *bucket
		for ; b != nil; b = bNext {
			if bNext, err = b.next(txn); err != nil || txn.RestartNeeded() {
				return err
			} else {
				emptied := true
				for idx, k := range ([][]byte)(*b.entries) {
					if b.isSlotEmpty(idx) {
						continue
					} else if lh.root.BucketIndex(lh.hash(k)) == sOld {
						emptied = false
					} else if _, _, chainDelta, err := bNew.put(txn, k, b.refs[idx+1]); err != nil || txn.RestartNeeded() {
						return err
					} else {
						lh.root.BucketCount += chainDelta
						([][]byte)(*b.entries)[idx] = nil
						b.refs[idx+1] = b.objRef
					}
				}

				if emptied {
					if bNext == nil {
						if bPrev == nil {
							// we have to keep b here, and there's no next,
							// so we have to write out b.
							b.tidyRefTail()
							if err := b.write(txn, true); err != nil || txn.RestartNeeded() {
								return err
							}
						} else {
							// we've detached b here, so will just wait to
							// write out bPrev
							lh.root.BucketCount--
							bPrev.refs[0] = bPrev.objRef
						}
					} else { // there is a next
						lh.root.BucketCount--
						if bPrev == nil {
							lh.refs[sOld] = bNext.objRef
						} else {
							bPrev.refs[0] = bNext.objRef
						}
					}
				} else {
					b.tidyRefTail()
					if bPrev != nil {
						if err := bPrev.write(txn, true); err != nil || txn.RestartNeeded() {
							return err
						}
					}
					bPrev = b
				}
			}
		}
		if bPrev != nil {
			if err := bPrev.write(txn, true); err != nil || txn.RestartNeeded() {
				return err
			}
		}
		return bNew.write(txn, true)
	}
}

func (lh *LHash) write(txn *client.Transaction) (err error) {
	if lh.value, err = lh.root.UpdateRaw().MarshalMsg(lh.value[:0]); err != nil {
		return err
	} else {
		// fmt.Println("write ->", lh.value)
		// fmt.Printf("write %#v, %v %v\n", lh.root, lh.k0, lh.k1)
		return txn.Write(lh.ObjRef, lh.value, lh.refs...)
	}
}

type bucket struct {
	*LHash
	objRef  client.RefCap
	entries *mp.Bucket
	value   []byte
	refs    []client.RefCap
}

func (lh *LHash) newBucket(txn *client.Transaction, objRef client.RefCap) (*bucket, error) {
	b := &bucket{
		LHash:  lh,
		objRef: objRef,
	}
	if err := b.populate(txn); err != nil || txn.RestartNeeded() {
		return nil, err
	} else {
		return b, nil
	}
}

func (lh *LHash) newEmptyBucket(objRef client.RefCap) *bucket {
	nextKeys := make([][]byte, mp.BucketCapacity)
	return &bucket{
		LHash:   lh,
		objRef:  objRef,
		entries: (*mp.Bucket)(&nextKeys),
		value:   nil,
		refs:    []client.RefCap{objRef},
	}
}

func (b *bucket) populate(txn *client.Transaction) error {
	if value, refs, err := txn.Read(b.objRef); err != nil || txn.RestartNeeded() {
		return err
	} else {
		entries := new(mp.Bucket)
		if _, err := entries.UnmarshalMsg(value); err != nil {
			return err
		}
		b.value = value
		b.entries = entries
		b.refs = refs
		return nil
	}
}

func (b *bucket) find(txn *client.Transaction, key []byte) (*client.RefCap, error) {
	for idx, k := range ([][]byte)(*b.entries) {
		if b.isSlotEmpty(idx) {
			continue
		} else if bytes.Equal(key, k) {
			return &b.refs[idx+1], nil
		}
	}

	if bNext, err := b.next(txn); err != nil {
		return nil, err
	} else if bNext != nil || txn.RestartNeeded() {
		return bNext.find(txn, key)
	} else {
		return nil, nil
	}
}

func (b *bucket) put(txn *client.Transaction, key []byte, value client.RefCap) (bNew *bucket, added bool, chainDelta int64, err error) {
	slot := -1
	for idx, k := range ([][]byte)(*b.entries) {
		if b.isSlotEmpty(idx) {
			if slot == -1 {
				// we've found a hole for it, let's use it. But we can
				// only use it if we're sure it's not already in this
				// bucket.
				slot = idx
			}
		} else if bytes.Equal(key, k) {
			b.refs[idx+1] = value
			// we didn't change any keys so don't need to serialize
			if err := b.write(txn, false); err != nil || txn.RestartNeeded() {
				return nil, false, 0, err
			} else {
				return b, false, 0, nil
			}
		}
	}

	if slot == -1 {
		return b.putInNext(txn, key, value)

	} else {
		return b.putInSlot(txn, key, value, slot)
	}
}

func (b *bucket) putInSlot(txn *client.Transaction, key []byte, value client.RefCap, slot int) (bNew *bucket, added bool, chainDelta int64, err error) {
	(*b.entries)[slot] = key
	slot++
	if slot == len(b.refs) {
		b.refs = append(b.refs, value)
	} else {
		b.refs[slot] = value
	}

	if next, err := b.next(txn); err != nil || txn.RestartNeeded() {
		return nil, false, 0, err

	} else if next != nil {
		next, removed, chainDelta, err := next.remove(txn, key)
		if err != nil || txn.RestartNeeded() {
			return nil, false, 0, err
		} else if next == nil {
			b.refs[0] = b.objRef
		} else {
			b.refs[0] = next.objRef
		}

		if err := b.write(txn, true); err != nil || txn.RestartNeeded() {
			return nil, false, 0, err
		} else {
			return b, !removed, chainDelta, nil
		}
	} else {
		if err := b.write(txn, true); err != nil || txn.RestartNeeded() {
			return nil, false, 0, err
		} else {
			return b, true, 0, nil
		}
	}
}

func (b *bucket) putInNext(txn *client.Transaction, key []byte, value client.RefCap) (bNew *bucket, added bool, chainDelta int64, err error) {
	if next, err := b.next(txn); err != nil || txn.RestartNeeded() {
		return nil, false, 0, err

	} else if next != nil {
		// next cannot change here
		if _, added, chainDelta, err := next.put(txn, key, value); err != nil || txn.RestartNeeded() {
			return nil, false, 0, err
		} else {
			return b, added, chainDelta, nil
		}

	} else {
		if objRef, err := txn.Create(nil); err != nil || txn.RestartNeeded() {
			return nil, false, 0, err
		} else {
			bNext := b.newEmptyBucket(objRef)
			if bNext, added, chainDelta, err := bNext.put(txn, key, value); err != nil || txn.RestartNeeded() {
				return nil, false, 0, err
			} else {
				b.refs[0] = bNext.objRef
				// we didn't change any keys so don't need to serialize
				if err := b.write(txn, false); err != nil || txn.RestartNeeded() {
					return nil, false, 0, err
				} else {
					return b, added, chainDelta + 1, nil
				}
			}
		}
	}
}

func (b *bucket) remove(txn *client.Transaction, key []byte) (bNew *bucket, removed bool, chainDelta int64, err error) {
	slot := -1
	for idx, k := range ([][]byte)(*b.entries) {
		if !b.isSlotEmpty(idx) && bytes.Equal(key, k) {
			slot = idx
			break
		}
	}

	if slot == -1 {
		if next, err := b.next(txn); err != nil || txn.RestartNeeded() {
			return nil, false, 0, err
		} else if next != nil {
			if next, removed, chainDelta, err := next.remove(txn, key); err != nil || txn.RestartNeeded() {
				return nil, false, 0, err
			} else if next == nil {
				b.refs[0] = b.objRef
				return b, removed, chainDelta, b.write(txn, false)
			} else if !b.refs[0].SameReferent(next.objRef) { // changed!
				b.refs[0] = next.objRef
				return b, removed, chainDelta, b.write(txn, false)
			} else {
				return b, removed, chainDelta, nil
			}
		} else {
			return b, false, 0, nil
		}

	} else {
		(*b.entries)[slot] = nil
		slot++
		b.refs[slot] = b.objRef
		b.tidyRefTail()
		if len(b.refs) == 1 { // we're empty; don't need to write us, just disconnect us.
			if next, err := b.next(txn); err != nil || txn.RestartNeeded() {
				return nil, false, 0, err
			} else {
				return next, true, -1, nil
			}
		} else {
			if err := b.write(txn, true); err != nil || txn.RestartNeeded() {
				return nil, false, 0, err
			} else {
				return b, true, 0, nil
			}
		}
	}
}

func (b *bucket) forEach(txn *client.Transaction, f func([]byte, client.RefCap) error) error {
	for idx, k := range ([][]byte)(*b.entries) {
		if b.isSlotEmpty(idx) {
			continue
		}
		if err := f(k, b.refs[idx+1]); err != nil {
			return err
		}
	}
	if bNext, err := b.next(txn); err != nil || txn.RestartNeeded() {
		return err
	} else if bNext != nil {
		return bNext.forEach(txn, f)
	} else {
		return nil
	}
}

func (b *bucket) tidyRefTail() {
	idx := len(b.refs) - 1
	for ; idx > 0 && b.objRef.SameReferent(b.refs[idx]); idx-- {
	}
	b.refs = b.refs[:idx+1]
}

func (b *bucket) write(txn *client.Transaction, updateEntries bool) (err error) {
	if updateEntries {
		if b.value, err = b.entries.MarshalMsg(b.value[:0]); err != nil {
			return err
		}
	}
	return txn.Write(b.objRef, b.value, b.refs...)
}

func (b *bucket) next(txn *client.Transaction) (*bucket, error) {
	if b.refs[0].SameReferent(b.objRef) {
		return nil, nil
	} else {
		return b.newBucket(txn, b.refs[0])
	}
}

func (b *bucket) isSlotEmpty(idx int) bool {
	return idx+1 >= len(b.refs) || b.refs[idx+1].SameReferent(b.objRef)
}
