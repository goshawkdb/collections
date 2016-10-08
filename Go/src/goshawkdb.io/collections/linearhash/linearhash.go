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
	// The connection used to create this LHash object. As usual with
	// GoshawkDB, objects are scoped to connections so you should not
	// use the same LHash object from multiple connections. You can
	// have multiple LHash objects for the same underlying set of
	// GoshawkDB objects.
	Conn *client.Connection
	// The underlying Object in GoshawkDB which holds the root data for
	// the LHash.
	ObjRef client.ObjectRef
	root   *mp.Root
	value  []byte
	refs   []client.ObjectRef
	k0     uint64
	k1     uint64
}

// Create a brand new empty LHash. This creates a new GoshawkDB Object
// and initialises it for use as an LHash.
func NewEmptyLHash(conn *client.Connection) (*LHash, error) {
	res, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		rootObjRef, err := txn.CreateObject([]byte{})
		if err != nil {
			return nil, err
		}

		lh := LHashFromObj(conn, rootObjRef)
		lh.root = new(mp.Root)
		lh.root.Size = 0
		lh.root.BucketCount = 2
		lh.root.SplitIndex = 0
		lh.root.MaskHigh = 3
		lh.root.MaskLow = 1

		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		key := make([]byte, 16)
		rng.Read(key)
		lh.root.Hashkey = key

		refs := make([]client.ObjectRef, lh.root.BucketCount)
		lh.refs = refs
		for idx := range refs {
			objRef, err := txn.CreateObject([]byte{})
			if err != nil {
				return nil, err
			}
			refs[idx] = objRef
			err = lh.newEmptyBucket(objRef).write(true)
			if err != nil {
				return nil, err
			}
		}

		return lh, lh.write()
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
func LHashFromObj(conn *client.Connection, objRef client.ObjectRef) *LHash {
	return &LHash{
		Conn:   conn,
		ObjRef: objRef,
	}
}

func (lh *LHash) populate() error {
	_, _, err := lh.Conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		obj, err := txn.GetObject(lh.ObjRef)
		if err != nil {
			return nil, err
		}
		lh.ObjRef = obj
		value, refs, err := obj.ValueReferences()
		if err != nil {
			return nil, err
		}
		// fmt.Println("read ->", value)
		root := new(mp.Root)
		_, err = root.UnmarshalMsg(value)
		if err != nil {
			return nil, err
		}
		lh.root = root
		lh.value = value
		lh.refs = refs
		lh.k0 = binary.LittleEndian.Uint64(root.Hashkey[0:8])
		lh.k1 = binary.LittleEndian.Uint64(root.Hashkey[8:16])
		// fmt.Printf("read %#v, %v %v\n", lh.root, lh.k0, lh.k1)
		return nil, nil
	})
	if err != nil {
		lh.root = nil
		lh.value = nil
		lh.refs = nil
		lh.k0 = 0
		lh.k1 = 0
	}
	return err
}

func (lh *LHash) hash(key []byte) uint64 {
	return hash.Hash(lh.k0, lh.k1, key)
}

// Search the LHash for the given key. The key is hashed using the
// SipHash algorithm, and comparison between keys is done with
// bytes.Equal. If no matching key is found, a nil ObjectRef is
// returned.
func (lh *LHash) Find(key []byte) (*client.ObjectRef, error) {
	res, _, err := lh.Conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		err := lh.populate()
		if err != nil {
			return nil, err
		}
		bucket, err := lh.newBucket(lh.refs[lh.root.BucketIndex(lh.hash(key))])
		if err != nil {
			return nil, err
		}
		return bucket.find(key)
	})
	if err == nil {
		return res.(*client.ObjectRef), nil
	} else {
		return nil, err
	}
}

// Idempotently add the given key and value to the LHash. The key is
// hashed using the SipHash algorithm, and comparison between keys is
// done with bytes.Equal. If a matching key is found, the
// corresponding value is updated.
func (lh *LHash) Put(key []byte, value client.ObjectRef) error {
	_, _, err := lh.Conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		err := lh.populate()
		if err != nil {
			return nil, err
		}
		bucket, err := lh.newBucket(lh.refs[lh.root.BucketIndex(lh.hash(key))])
		if err != nil {
			return nil, err
		}
		_, added, chainDelta, err := bucket.put(key, value)
		if err != nil {
			return nil, err
		}
		// fmt.Printf("(%v) Put %v, added:%v; chainDelta:%v\n", lh.root.Size, key, added, chainDelta)
		if added || chainDelta != 0 {
			if added {
				lh.root.Size++
			}
			lh.root.BucketCount += chainDelta
			if lh.root.NeedsSplit() {
				err = lh.split()
				if err != nil {
					return nil, err
				}
			}
			return nil, lh.write()
		}
		return nil, nil
	})
	return err
}

// Idempotently remove any matching entry from the LHash. The key is
// hashed using the SipHash algorithm, and comparison between keys is
// done with bytes.Equal.
func (lh *LHash) Remove(key []byte) error {
	_, _, err := lh.Conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		err := lh.populate()
		if err != nil {
			return nil, err
		}
		idx := lh.root.BucketIndex(lh.hash(key))
		bucket, err := lh.newBucket(lh.refs[idx])
		if err != nil {
			return nil, err
		}
		bNew, removed, chainDelta, err := bucket.remove(key)
		if err != nil {
			return nil, err
		}
		if removed || chainDelta != 0 {
			if bNew == nil { // must keep old bucket even though it's empty
				err = bucket.write(true)
				if err != nil {
					return nil, err
				}
			} else if bNew != bucket {
				lh.refs[idx] = bNew.objRef
			}
			if removed {
				lh.root.Size--
			}
			lh.root.BucketCount += chainDelta
			return nil, lh.write()
		}
		return nil, nil
	})
	return err
}

// Returns the number of entries in the LHash.
func (lh *LHash) Size() (int64, error) {
	res, _, err := lh.Conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		err := lh.populate()
		if err != nil {
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

func (lh *LHash) split() error {
	sOld := lh.root.SplitIndex
	b, err := lh.newBucket(lh.refs[sOld])
	if err != nil {
		return err
	}
	res, _, err := lh.Conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		return txn.CreateObject([]byte{})
	})
	if err != nil {
		return err
	}
	bNew := lh.newEmptyBucket(res.(client.ObjectRef))
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
		bNext, err = b.next()
		if err != nil {
			return err
		}
		emptied := true
		for idx, k := range ([][]byte)(*b.entries) {
			if len(k) == 0 {
				continue
			} else if lh.root.BucketIndex(lh.hash(k)) == sOld {
				emptied = false
			} else {
				_, _, chainDelta, err := bNew.put(k, b.refs[idx+1])
				if err != nil {
					return err
				}
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
					err = b.write(true)
					if err != nil {
						return err
					}
				} else {
					// we've detached b here, so will just wait to
					// write out bPrev
					lh.root.BucketCount--
					bPrev.refs[0] = bPrev.objRef
				}
			} else { // there is a next
				if bPrev == nil {
					lh.root.BucketCount--
					lh.refs[sOld] = bNext.objRef
				} else {
					lh.root.BucketCount--
					bPrev.refs[0] = bNext.objRef
				}
			}
		} else {
			b.tidyRefTail()
			if bPrev != nil {
				err = bPrev.write(true)
				if err != nil {
					return err
				}
			}
			bPrev = b
		}
	}
	if bPrev != nil {
		err = bPrev.write(true)
		if err != nil {
			return err
		}
	}
	return bNew.write(true)
}

func (lh *LHash) write() (err error) {
	lh.value, err = lh.root.MarshalMsg(lh.value[:0])
	if err != nil {
		return
	}
	// fmt.Println("write ->", lh.value)
	// fmt.Printf("write %#v, %v %v\n", lh.root, lh.k0, lh.k1)
	return lh.ObjRef.Set(lh.value, lh.refs...)
}

type bucket struct {
	*LHash
	objRef  client.ObjectRef
	entries *mp.Bucket
	value   []byte
	refs    []client.ObjectRef
}

func (lh *LHash) newBucket(objRef client.ObjectRef) (*bucket, error) {
	b := &bucket{
		LHash:  lh,
		objRef: objRef,
	}
	if err := b.populate(); err == nil {
		return b, nil
	} else {
		return nil, err
	}
}

func (lh *LHash) newEmptyBucket(objRef client.ObjectRef) *bucket {
	nextKeys := make([][]byte, mp.BucketCapacity)
	return &bucket{
		LHash:   lh,
		objRef:  objRef,
		entries: (*mp.Bucket)(&nextKeys),
		value:   nil,
		refs:    []client.ObjectRef{objRef},
	}
}

func (b *bucket) populate() error {
	_, _, err := b.Conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		obj, err := txn.GetObject(b.objRef)
		if err != nil {
			return nil, err
		}
		b.objRef = obj
		value, refs, err := obj.ValueReferences()
		if err != nil {
			return nil, err
		}
		entries := new(mp.Bucket)
		_, err = entries.UnmarshalMsg(value)
		if err != nil {
			return nil, err
		}
		b.value = value
		b.entries = entries
		b.refs = refs
		return nil, nil
	})
	if err != nil {
		b.entries = nil
		b.value = nil
		b.refs = nil
	}
	return err
}

func (b *bucket) find(key []byte) (*client.ObjectRef, error) {
	for idx, k := range ([][]byte)(*b.entries) {
		if len(k) == 0 {
			continue
		} else if bytes.Equal(key, k) {
			return &b.refs[idx+1], nil
		}
	}

	if bNext, err := b.next(); err != nil {
		return nil, err
	} else if bNext != nil {
		return bNext.find(key)
	} else {
		return nil, nil
	}
}

func (b *bucket) put(key []byte, value client.ObjectRef) (bNew *bucket, added bool, chainDelta int64, err error) {
	slot := -1
	for idx, k := range ([][]byte)(*b.entries) {
		if len(k) == 0 {
			if slot == -1 {
				// we've found a hole for it, let's use it. But we can
				// only use it if we're sure it's not already in this
				// bucket.
				slot = idx
			}
		} else if bytes.Equal(key, k) {
			b.refs[idx+1] = value
			// we didn't change any keys so don't need to serialize
			err = b.write(false)
			if err == nil {
				return b, false, 0, nil
			} else {
				return
			}
		}
	}

	if slot == -1 {
		return b.putInNext(key, value)

	} else {
		return b.putInSlot(key, value, slot)
	}
}

func (b *bucket) putInSlot(key []byte, value client.ObjectRef, slot int) (bNew *bucket, added bool, chainDelta int64, err error) {
	(*b.entries)[slot] = key
	slot++
	if slot == len(b.refs) {
		b.refs = append(b.refs, value)
	} else {
		b.refs[slot] = value
	}

	var next *bucket
	if next, err = b.next(); err != nil {
		return

	} else if next != nil {
		removed := false
		next, removed, chainDelta, err = next.remove(key)
		if err != nil {
			return
		}
		if next == nil {
			b.refs[0] = b.objRef
		} else {
			b.refs[0] = next.objRef
		}
		err = b.write(true)
		if err != nil {
			return
		}
		return b, !removed, chainDelta, nil

	} else {
		err = b.write(true)
		if err != nil {
			return
		}
		return b, true, 0, nil
	}
}

func (b *bucket) putInNext(key []byte, value client.ObjectRef) (bNew *bucket, added bool, chainDelta int64, err error) {
	var next *bucket
	if next, err = b.next(); err != nil {
		return

	} else if next != nil {
		// next cannot change here
		_, added, chainDelta, err = next.put(key, value)
		if err != nil {
			return
		}
		return b, added, chainDelta, nil

	} else {
		var res interface{}
		res, _, err = b.Conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
			return txn.CreateObject([]byte{})
		})
		if err != nil {
			return
		}
		bNext := b.newEmptyBucket(res.(client.ObjectRef))
		bNext, added, chainDelta, err = bNext.put(key, value)
		if err != nil {
			return
		}
		b.refs[0] = bNext.objRef
		// we didn't change any keys so don't need to serialize
		err = b.write(false)
		if err == nil {
			return b, added, chainDelta + 1, nil
		} else {
			return
		}
	}
}

func (b *bucket) remove(key []byte) (bNew *bucket, removed bool, chainDelta int64, err error) {
	slot := -1
	for idx, k := range ([][]byte)(*b.entries) {
		if len(k) == 0 {
			continue
		} else if bytes.Equal(key, k) {
			slot = idx
			break
		}
	}

	if slot == -1 {
		var next *bucket
		if next, err = b.next(); err != nil {
			return
		} else if next != nil {
			next, removed, chainDelta, err = next.remove(key)
			if err != nil {
				return
			}
			if next == nil {
				b.refs[0] = b.objRef
				err = b.write(false)
			} else if !b.refs[0].ReferencesSameAs(next.objRef) { // changed!
				b.refs[0] = next.objRef
				err = b.write(false)
			}
			if err != nil {
				return
			}
			return b, removed, chainDelta, nil
		} else {
			return b, false, 0, nil
		}

	} else {
		(*b.entries)[slot] = nil
		slot++
		b.refs[slot] = b.objRef
		b.tidyRefTail()
		if len(b.refs) == 1 { // we're empty; don't need to write us, just disconnect us.
			var next *bucket
			next, err = b.next()
			if err == nil {
				return next, true, -1, nil
			} else {
				return
			}
		} else {
			err = b.write(true)
			if err == nil {
				return b, true, 0, nil
			} else {
				return
			}
		}
	}
}

func (b *bucket) tidyRefTail() {
	idx := len(b.refs) - 1
	for ; idx > 0 && b.objRef.ReferencesSameAs(b.refs[idx]); idx-- {
	}
	b.refs = b.refs[:idx+1]
}

func (b *bucket) write(updateEntries bool) (err error) {
	if updateEntries {
		b.value, err = b.entries.MarshalMsg(b.value[:0])
		if err != nil {
			return err
		}
	}
	return b.objRef.Set(b.value, b.refs...)
}

func (b *bucket) next() (*bucket, error) {
	if b.refs[0].ReferencesSameAs(b.objRef) {
		return nil, nil
	} else {
		return b.newBucket(b.refs[0])
	}
}
