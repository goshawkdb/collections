package io.goshawkdb.collections.linearhash;

import com.zackehh.siphash.SipHash;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.function.BiConsumer;

import io.goshawkdb.client.RefCap;
import io.goshawkdb.client.Transaction;
import io.goshawkdb.client.Transactor;
import io.goshawkdb.client.ValueRefs;

/**
 * A LinearHash is a map structure using the linear-hashing algorithm. This implementation uses the SipHash hashing algorithm,
 * which is available on many platforms and languages, and uses msgpack as the serialization format for the LinearHash state,
 * which again, is widely available.
 *
 * Multiple connections may interact with the same underlying LHash objects at the same time, as GoshawkDB ensures through the
 * use of strong serialization that any dependent operations are safely ordered.
 */
public class LinearHash {
    /**
     * The underlying Object in GoshawkDB which holds the root data for the LHash.
     */
    public RefCap objRef;

    private Root root;
    private RefCap[] refs;
    private SipHash sipHash;

    /**
     * Create a LinearHash object from an existing given GoshawkDB Object. Use this to regain access to an existing LinearHash
     * which has already been created. This function does not do any initialisation: it assumes the {@link RefCap} passed is
     * already initialised for LinearHash.
     *
     * @param ref The {@link RefCap} which contains the root state of the {@link LinearHash}
     */
    public LinearHash(final RefCap ref) {
        objRef = ref;
    }

    /**
     * Create a brand new empty LinearHash. This creates a new GoshawkDB Object and initialises it for use as an LHash.
     *
     * @return The new {@link LinearHash}
     */
    public static LinearHash createEmpty(final Transactor txr) {
        final LinearHash lh = txr.transact(txn -> {
            final RefCap rootObjRef = txn.create(null);
            if (txn.restartNeeded()) {
                return null;
            }
            final LinearHash lhTxn = new LinearHash(rootObjRef);
            lhTxn.root = new Root();

            final RefCap[] refs = new RefCap[lhTxn.root.bucketCount];
            lhTxn.refs = refs;
            for (int idx = 0; idx < refs.length; idx++) {
                final RefCap objRef = txn.create(null);
                if (txn.restartNeeded()) {
                    return null;
                }
                refs[idx] = objRef;
                Bucket.createEmpty(lhTxn, objRef).write(txn, true);
                if (txn.restartNeeded()) {
                    return null;
                }
            }

            lhTxn.write(txn);
            if (txn.restartNeeded()) {
                return null;
            }
            return lhTxn;
        }).getResultOrRethrow();
        lh.sipHash = new SipHash(lh.root.hashkey);
        return lh;
    }

    private void populate(final Transaction txn) {
        final ValueRefs objVR = txn.read(objRef);
        if (txn.restartNeeded()) {
            return;
        }
        refs = objVR.references;
        root = new Root(objVR.value);
        sipHash = new SipHash(root.hashkey);
    }

    private void write(final Transaction txn) {
        txn.write(objRef, root.pack(), refs);
    }

    private BigInteger hash(final byte[] key) {
        return new BigInteger(sipHash.hash(key).getHex(), 16);
    }

    /**
     * Search the LinearHash for the given key. The key is hashed using the SipHash algorithm, and comparison between keys is
     * done with {@link Arrays}.equals. If no matching key is found, null is returned.
     *
     * @param key The key to search for
     * @return The corresponding value if the key is found; null otherwise.
     */
    public RefCap find(final Transactor txr, final byte[] key) {
        return txr.transact(txn -> {
            populate(txn);
            if (txn.restartNeeded()) {
                return null;
            }
            final Bucket b = Bucket.load(txn, this, refs[root.bucketIndex(hash(key))]);
            if (txn.restartNeeded()) {
                return null;
            }
            return b.find(txn, key);
        }).getResultOrRethrow();
    }

    /**
     * Idempotently add the given key and value to the LinearHash. The key is hashed using the SipHash algorithm, and
     * comparison between keys is done with {@link Arrays}.equals. If a matching key is found, the corresponding value is
     * updated.
     *
     * @param key   The key to search for and add.
     * @param value The value to associate with the key.
     * @return A transaction result with no value that captures any errors that occurred
     */
    public void put(final Transactor txr, final byte[] key, final RefCap value) {
        txr.transact(txn -> {
            populate(txn);
            if (txn.restartNeeded()) {
                return null;
            }
            final Bucket b = Bucket.load(txn, this, refs[root.bucketIndex(hash(key))]);
            if (txn.restartNeeded()) {
                return null;
            }
            final Bucket.ChainMutationResult cmr = b.put(txn, key, value);
            if (txn.restartNeeded()) {
                return null;
            } else if (cmr.done || cmr.chainDelta != 0) {
                if (cmr.done) {
                    root.size++;
                }
                root.bucketCount += cmr.chainDelta;
                if (root.needsSplit()) {
                    split(txn);
                    if (txn.restartNeeded()) {
                        return null;
                    }
                }
                write(txn);
            }
            return null;
        }).getResultOrRethrow();
    }

    /**
     * Idempotently remove any matching entry from the LinearHash. The key is hashed using the SipHash algorithm, and
     * comparison between keys is done with {@link Arrays}.equals.
     *
     * @param key The to search for and remove.
     * @return A transaction result with no value that captures any errors that occurred
     */
    public void remove(final Transactor txr, final byte[] key) {
        txr.transact(txn -> {
            populate(txn);
            if (txn.restartNeeded()) {
                return null;
            }
            final int idx = root.bucketIndex(hash(key));
            final Bucket b = Bucket.load(txn, this, refs[idx]);
            if (txn.restartNeeded()) {
                return null;
            }
            final Bucket.ChainMutationResult cmr = b.remove(txn, key);
            if (txn.restartNeeded()) {
                return null;
            } else if (cmr.done || cmr.chainDelta != 0) {
                if (cmr.b == null) { // must keep old bucket even though it's empty
                    b.write(txn, true);
                    if (txn.restartNeeded()) {
                        return null;
                    }
                } else if (cmr.b != b) {
                    refs[idx] = cmr.b.objRef;
                }
                if (cmr.done) {
                    root.size--;
                }
                root.bucketCount += cmr.chainDelta;
                write(txn);
            }
            return null;
        }).getResultOrRethrow();
    }

    /**
     * Iterate over the entries in the LinearHash. Iteration order is undefined. Also note that as usual, the transaction in
     * which the iteration is occurring may need to restart one or more times in which case the callback may be invoked several
     * times for the same entry. To detect this, call forEach from within a transaction of your own. Iteration will stop as
     * soon as the callback throws an error, which will also abort the transaction.
     *
     * @param action The action to be performed for each entry
     * @return A transaction result with no value that captures any errors that occurred
     */
    public void forEach(Transactor txr, BiConsumer<? super byte[], ? super RefCap> action) {
        txr.transact(txn -> {
            populate(txn);
            if (txn.restartNeeded()) {
                return null;
            }
            for (RefCap objRef : refs) {
                final Bucket b = Bucket.load(txn, this, objRef);
                if (txn.restartNeeded()) {
                    return null;
                }
                b.forEach(txn, action);
                if (txn.restartNeeded()) {
                    return null;
                }
            }
            return null;
        }).getResultOrRethrow();
    }

    /**
     * Returns the number of entries in the LinearHash.
     *
     * @return the number of entries in the LinearHash.
     */
    public int size(final Transactor txr) {
        return txr.transact(txn -> {
            populate(txn);
            if (txn.restartNeeded()) {
                return null;
            }
            return root.size;
        }).getResultOrRethrow();
    }

    private void split(final Transaction txn) {
        final int sOld = root.splitIndex.intValueExact();
        Bucket b = Bucket.load(txn, this, refs[sOld]);
        if (txn.restartNeeded()) {
            return;
        }
        final Bucket bNew = Bucket.createEmpty(this, txn.create(null));
        if (txn.restartNeeded()) {
            return;
        }

        refs = Arrays.copyOf(refs, refs.length + 1);
        refs[refs.length - 1] = bNew.objRef;
        root.bucketCount++;
        root.splitIndex = root.splitIndex.add(BigInteger.ONE);

        if (root.splitIndex.shiftLeft(1).compareTo(BigInteger.valueOf(refs.length)) == 0) {
            // we've split everything
            root.splitIndex = BigInteger.ZERO;
            root.maskLow = root.maskHigh;
            root.maskHigh = root.maskHigh.shiftLeft(1).add(BigInteger.ONE);
        }

        Bucket bPrev = null, bNext = null;
        for (; b != null; b = bNext) {
            bNext = b.next(txn);
            if (txn.restartNeeded()) {
                return;
            }
            boolean emptied = true;
            for (int idx = 0; idx < b.entries.length; idx++) {
                if (b.isSlotEmpty(idx)) {
                    continue;
                } else if (root.bucketIndex(hash(b.entries[idx])) == sOld) {
                    emptied = false;
                } else {
                    final Bucket.ChainMutationResult cmr = bNew.put(txn, b.entries[idx], b.refs.get(idx + 1));
                    if (txn.restartNeeded()) {
                        return;
                    }
                    root.bucketCount += cmr.chainDelta;
                    b.entries[idx] = null;
                    b.refs.set(idx + 1, b.objRef);
                }
            }

            if (emptied) {
                if (bNext == null) {
                    if (bPrev == null) {
                        // we have to keep b here, and there's no next,
                        // so we have to write out b.
                        b.tidyRefTail();
                        b.write(txn, true);
                        if (txn.restartNeeded()) {
                            return;
                        }
                    } else {
                        // we've detached b here, so will just wait to
                        // write out bPrev
                        root.bucketCount--;
                        bPrev.refs.set(0, bPrev.objRef);
                    }
                } else { // there is a next
                    root.bucketCount--;
                    if (bPrev == null) {
                        refs[sOld] = bNext.objRef;
                    } else {
                        bPrev.refs.set(0, bNext.objRef);
                    }
                }
            } else {
                b.tidyRefTail();
                if (bPrev != null) {
                    bPrev.write(txn, true);
                    if (txn.restartNeeded()) {
                        return;
                    }
                }
                bPrev = b;
            }
        }
        if (bPrev != null) {
            bPrev.write(txn, true);
            if (txn.restartNeeded()) {
                return;
            }
        }
        bNew.write(txn, true);
        if (txn.restartNeeded()) {
            return;
        }
    }
}