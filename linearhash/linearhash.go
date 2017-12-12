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

// Implementation notes
//
// The Linear Hash is a relatively old data structure and many of the
// original papers concern themselves with optimising layout on disk
// for disk seeks, and so are not the ideal documentation. Some of the
// other online resources for linear hashes are simply awful. Hence
// these notes.
//
// As the name suggests, we need a hash function with some good
// properties: uniform random distribution of keys to hashcodes, and
// ideally immune to some of the hash poisoning attacks that have been
// seen in the past few years. Siphash seems to be the default choice
// these days and so we go with that. However, Siphash is pretty
// interesting in that it is parameterised by a 128-bit key. This key
// must be stored with the linear hash (lhash from here on) data
// structure so that it is consistently used for all hashing
// operations. So for a new lhash, we just create a [16]byte array and
// fill it with random data (see NewEmptyLHash below). The actual
// SipHash API in Go wants two 64-bit uints, so we are just a little
// careful when loading out of GoshawkDB to get that all set up
// correctly (endian concerns etc (see LHash.populate below)).
//
// So what is the siphash used for? The siphash hashes the key that we
// either wish to search for, or insert, or remove from the lhash.
// The result of the siphash (the hashcode) indicates which bucket we
// start searching in. What what what?
//
// Some definitions:
//
// - In this implementation, we declare that a bucket can contain no
//     more than 64 keys (see msgpack.BucketCapacity).
// - A key is just a byte-array (of unlimited length).
// - The bucket is just a msgpack array of keys.
// - The utilisation factor is 0.75 (see msgpack.UtilizationFactor).
//
// Initial state:
//
// fig 1.         [A] [B]
//
// The initial state of an lhash consists of two buckets, shown above:
// A and B. When we are given a key, we need to decide which bucket we
// think the key is in. So, we hash the key, and then in this simple
// state, we just look at the least significant bit of the
// hashcode. If it's a 0, then we work in bucket A, if it's a 1 then
// bucket B. If you look at the Root.BucketIndex function then the
// code there is more complex, but also look at NewRoot where we learn
// the initial SplitIndex is 0, so we are always taking the first
// branch of the conditional in BucketIndex for now, and so we are
// just doing the hashcode bitwise-AND with MaskLow, which starts life
// as 1. So we really are looking at the least significant bit only.
//
// When we insert (Put) a key-value pair, we first check the entire
// bucket to see if we can find the key already in there. If we do,
// then we just need to update the value. If the key is found in slot
// i within bucket A then the value is stored in the i+1 reference of
// bucket A. This is because the 0th reference is special - this will
// be explained shortly. If, when we do a Put, we can't find the key,
// we add the key in the first empty slot within the bucket, and link
// to the value as described. Keys are not ordered within buckets.
//
// As we all know, life can be very unfair. So we can imagine a
// situation where one bucket is filled up whilst the other is left
// completely empty. For example, bucket B may be totally filled. We
// now want to do a further Put, and as luck would have it, our
// BucketIndex function again indicates we want bucket B. So we create
// a chained bucket:
//
// fig 2.         [A] [B]
//                     |
//                    [C]
//
// Bucket C is chained from bucket B. The special 0th reference of B
// points to C. The special 0th reference of C points to C. So: if a
// bucket X has another bucket chained to it Y then the 0th reference
// of X = &Y. Otherwise, the 0th reference of X = &X. Thus when
// traversing a chain, we can detect when we get to the end (nil
// references are not supported in GoshawkDB, hence this
// construction).
//
// So in this scenario, the only thing that has changed is that now
// once we're done searching B, we have to continue on and search C
// too. This introduces two further complexities:
//
// 1a. When removing items, it's possible that we remove everything
// from C. This means we have to correct the 0th reference from B to
// point back at itself and fully detach C. (back to figure 1)
//
// 1b. The opposite can also happen: when removing items, it's
// possible that we empty out B, but we need to keep C. In this case,
// B is removed from the chain (and the lhash) and C now takes the
// place of B. (figure 3)
//
// fig 3.         [A] [C]
//
// 2. When inserting an item into the chain from B, we may find a
// suitable empty slot in B, which we then use. But it's possible that
// this key already exists further down the chain, so we then have to
// search C to attempt to remove that key. This can then cause C to
// now become empty, and so we're in the same situation as case 1a
// above.
//
// A chain of buckets can be any length >= 1 and so we have to cope
// with removing buckets from the start, the middle and the end of a
// chain. A new bucket is always added to the end of a chain, never
// the start or middle. If the chain is completely empty, it is a
// chain of length 1 and that first bucket in the chain contains no
// keys.
//
// For performance reasons, it's a good idea to avoid long chains
// which are partially full. Hence why an insert will always use the
// first available slot in the first non-full bucket of a chain
// (except when the bucket already contains the key). However, we do
// not do any further compaction of chains.
//
// A bucket starts off with zero keys in it. It does not start off
// with 64 empty keys in it. So, if nothing is removed from a bucket,
// the bucket will grow in size up to the limit of 64 keys, and 65
// references. When a key is removed from a bucket, this key may not
// be the last item in the bucket, so we have to have a way to
// indicate the key is absent (i.e. the slot is now empty). To
// indicate that slot i is empty, the i+1 reference will point to the
// bucket itself. See bucket.isSlotEmpty.
//
// One final complexity is that we try to reduce the size of a bucket
// where it is simple to do so. So if we detect we've removed the last
// key in a bucket, we will then reduce the size of the bucket back to
// the last non-empty slot (and reduce the number of references
// accordingly). Again, we don't do any complex compaction, we just do
// this one simple thing.
//
// With chains explained, there's not much left to cover.
//
// Splitting. When the utilization of the lhash is > the
// UtilizationFactor, we split a bucket. The utilization factor is a
// measure of how full the lhash is. It is the Size /
// (BucketCapacity*BucketCount), where BucketCount includes all
// buckets in all chains, and Size is the total number of keys in the
// lhash. In this implementation, UtilizationFactor is 0.75. A higher
// value will make the buckets more full but will produce longer
// chains; a lower value will reduce the average length of chains, but
// more splitting will happen. So it's a tradeoff. 0.75 is about
// right. Going back to figures 1 and 2, with a UtilizationFactor of
// 0.75, you can see how if bucket B is full and A is empty, then the
// utilization is 0.5, so rather than splitting, we will have to
// create bucket C and start a chain from B.
//
// So what is splitting? Say we're back at figure 1. But this time,
// both buckets A and B are filled equally until both have 48 entries
// in (utilisation is 0.75). Now we want to do a further put. Rather
// than creating a chain (in this case we wouldn't need to create a
// chain because neither bucket is full, but we choose to split
// nevertheless), we "split" the bucket pointed to by the
// SplitIndex. Initially, the SplitIndex is 0, so we split the 0th
// bucket (A) and end up with figure 4.
//
//                     S
// fig 4.         [A] [B] [C]
//
// The split index (S) has been incremented to 1 (so is now indicating
// bucket B is the next to be split). Now if we return to the
// BucketIndex function we have to consider both branches. The MaskLow
// and MaskHigh have not been changed. So, if the least significant
// bit of the hashcode of our key is >= the new SplitIndex (i.e. >= 1)
// then we directly use the least significant bit. Otherwise, we now
// have to use the hashcode bitwise-AND with MaskHigh, which starts
// off as 3. So we can draw up a truth table:
//
// hash code | LSB >= 1? | bucket index | bucket
//     ...00 | false     | 00           | A
//     ...01 | true      |  1           | B
//     ...10 | false     | 10           | C
//     ...11 | true      |  1           | B
//
// Bucket A has been split and now its hash codes are evenly split
// between the original bucket A, and the new bucket C. Nothing in
// bucket B is altered. So the split process needs to work through
// bucket A (and any buckets chained to bucket A) and move key-values
// to bucket C (potentially even creating a chain off C) as is now
// required from their hash codes. This does involve rehashing every
// key in the chain descending from A and moving some of them (on
// average, half of them) to bucket C. Again, we do no complex
// compaction of the chain from A; it's just a slightly optimised
// remove from A (optimised because we don't need to search for the
// key), and a totally normal put into C.
//
// Some time later, we once again find that our lhash has a
// utilisation > 0.75 and we want to split again. Now we are splitting
// B. Once we have split B, we have split both of our original
// buckets. We can clearly imagine further scenarios in the future
// where we need to split A and B again, and so now we:
//   i. Reset the SplitIndex to 0
//  ii. Set the new MaskLow to the old MaskHigh (i.e. 3 in this case)
// iii. Set the new MaskHigh to 2*MaskHigh+1 (i.e. 7 in this case)
//
// To be clear, the MaskLow is always 1 bit less than the MaskHigh -
// so here the MaskHigh is 7 which is the least 3 bits. MaskLow is 3,
// which is the least 2 bits.
//
//                 S
// fig 5.         [A] [B] [C] [D]
//
// Now, if we construct the same truth table as above, we find that
// rather than testing the LSB, we are testing the least significant 2
// bits as MaskLow is now 3, not 1:
//
// hash code | LS2 >= 0? | bucket index | bucket
//     ...00 | true      | 00           | A
//     ...01 | true      | 01           | B
//     ...10 | true      | 10           | C
//     ...11 | true      | 11           | D
//
// So we can see that bucket B has correctly been split and roughly
// half its contents will have moved into D. Nothing in A or C will
// have changed. We are now starting again, if you like, but with
// higher masks, and an original set of 4 buckets instead of 2.
//
// Following from this, we would now split A again (now creating E),
// then B again (creating F), then C (creating G), which would put us
// in this situation:
//
//                             S
// fig 6.         [A] [B] [C] [D] [E] [F] [G]
//
// MaskLow and MaskHigh are still 3 and 7 respectively.
//
// Once again, we've reached the point where we've split all our
// original buckets, and so when creating bucket H, we must also reset
// the SplitIndex to 0, update the MaskLow to 7 (i.e. least 3 bits),
// and the MaskHigh to 15 (i.e. least 4 bits). Now we start again, but
// with 8 original buckets. And so it goes on.
//
// From here, the generalisation may be clear: if we bitwise-AND the
// hashcode with the MaskLow and the result is the indicated bucket is
// to the left of the SplitIndex then we know we're pointing at a
// bucket that has already been split. So therefore we need to take
// another bit from the hashcode - i.e. use the MaskHigh, in order to
// correctly determine the bucket. But, if the bitwise-AND of hashcode
// with the MaskLow points either at or to the right of the SplitIndex
// then we know that bucket hasn't been split, so we're clear to
// proceed.
//
// What happens if the utilisation factor goes too low? Do we try to
// remove and "un-split" buckets? Nope. There's no such thing. I'm
// sure it's possible, but I've never seen any papers claiming that
// it's worth the complexity.

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
