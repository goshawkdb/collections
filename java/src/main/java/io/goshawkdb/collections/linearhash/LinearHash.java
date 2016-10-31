package io.goshawkdb.collections.linearhash;

import com.zackehh.siphash.SipHash;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.BiConsumer;

import io.goshawkdb.client.Connection;
import io.goshawkdb.client.GoshawkObjRef;
import io.goshawkdb.client.TransactionAbortedException;
import io.goshawkdb.client.TransactionResult;

/**
 * A LinearHash is a map structure using the linear-hashing
 * algorithm. This implementation uses the SipHash hashing algorithm,
 * which is available on many platforms and languages, and uses
 * msgpack as the serialization format for the LinearHash state, which
 * again, is widely available.
 *
 * Multiple connections may interact with the same underlying LHash
 * objects at the same time, as GoshawkDB ensures through the use of
 * strong serialization that any dependent operations are safely
 * ordered.
 */
public class LinearHash {
    /**
     * The connection used to create this LinearHash object. As usual with
     * GoshawkDB, objects are scoped to connections so you should not
     * use the same LinearHash object from multiple connections. You can
     * have multiple LinearHash objects for the same underlying set of
     * GoshawkDB objects.
     */
    public final Connection conn;
    /**
     * The underlying Object in GoshawkDB which holds the root data for
     * the LHash.
     */
    public GoshawkObjRef objRef;

    private Root root;
    private GoshawkObjRef[] refs;
    private SipHash sipHash;

    /**
     * Create a LinearHash object from an existing given GoshawkDB Object. Use
     * this to regain access to an existing LinearHash which has already been
     * created. This function does not do any initialisation: it assumes
     * the {@link GoshawkObjRef} passed is already initialised for LinearHash.
     *
     * @param c   The connection this {@link LinearHash} object will be scoped to.
     * @param ref The {@link GoshawkObjRef} which contains the root state of the {@link LinearHash}
     */
    public LinearHash(final Connection c, final GoshawkObjRef ref) {
        conn = c;
        objRef = ref;
    }

    /**
     * Create a brand new empty LinearHash. This creates a new GoshawkDB Object
     * and initialises it for use as an LHash.
     *
     * @param c The connection this {@link LinearHash} object will be scoped to.
     * @return The new {@link LinearHash}
     * @throws Exception if an unexpected error occurs during the transactions.
     */
    public static LinearHash createEmpty(final Connection c) throws Exception {
        final TransactionResult<LinearHash> result = c.runTransaction(txn -> {
            final GoshawkObjRef rootObjRef = txn.createObject(null);
            final LinearHash lh = new LinearHash(c, rootObjRef);
            lh.root = new Root();

            final GoshawkObjRef[] refs = new GoshawkObjRef[lh.root.bucketCount];
            lh.refs = refs;
            for (int idx = 0; idx < refs.length; idx++) {
                final GoshawkObjRef objRef = txn.createObject(null);
                refs[idx] = objRef;
                Bucket.createEmpty(lh, objRef).write(true);
            }

            lh.write();
            return lh;
        });
        if (result.isSuccessful()) {
            final LinearHash lh = result.result;
            lh.sipHash = new SipHash(lh.root.hashkey);
            return lh;
        } else {
            throw result.cause;
        }
    }

    private void populate() {
        TransactionResult<Object> result = conn.runTransaction(txn -> {
            objRef = txn.getObject(objRef);
            final ByteBuffer value = objRef.getValue();
            refs = objRef.getReferences();
            root = new Root(value);
            return null;
        });
        if (result.isSuccessful()) {
            sipHash = new SipHash(root.hashkey);
        } else {
            root = null;
            refs = null;
            sipHash = null;
            throw new TransactionAbortedException(result.cause);
        }
    }

    private void write() {
        objRef.set(root.pack(), refs);
    }

    private BigInteger hash(final byte[] key) {
        return new BigInteger(sipHash.hash(key).getHex(), 16);
    }

    /**
     * Search the LinearHash for the given key. The key is hashed using the
     * SipHash algorithm, and comparison between keys is done with
     * {@link Arrays}.equals. If no matching key is found, null is
     * returned.
     *
     * @param key The key to search for
     * @return The corresponding value if the key is found; null otherwise.
     * @throws Exception if an unexpected error occurs during the transactions.
     */
    public GoshawkObjRef find(final byte[] key) throws Exception {
        final TransactionResult<GoshawkObjRef> result = conn.runTransaction(txn -> {
            populate();
            final Bucket b = Bucket.load(this, refs[root.bucketIndex(hash(key))]);
            return b.find(key);
        });
        if (result.isSuccessful()) {
            return result.result;
        } else {
            throw result.cause;
        }
    }

    /**
     * Idempotently add the given key and value to the LinearHash. The key is
     * hashed using the SipHash algorithm, and comparison between keys is
     * done with {@link Arrays}.equals. If a matching key is found, the
     * corresponding value is updated.
     *
     * @param key   The key to search for and add.
     * @param value The value to associate with the key.
     * @throws Exception if an unexpected error occurs during the transactions.
     */
    public void put(final byte[] key, final GoshawkObjRef value) throws Exception {
        TransactionResult<Object> result = conn.runTransaction(txn -> {
            populate();
            final Bucket b = Bucket.load(this, refs[root.bucketIndex(hash(key))]);
            final Bucket.ChainMutationResult cmr = b.put(key, value);
            if (cmr.done || cmr.chainDelta != 0) {
                if (cmr.done) {
                    root.size++;
                }
                root.bucketCount += cmr.chainDelta;
                if (root.needsSplit()) {
                    split();
                }
                write();
            }
            return null;
        });
        if (!result.isSuccessful()) {
            throw result.cause;
        }
    }

    /**
     * Idempotently remove any matching entry from the LinearHash. The key is
     * hashed using the SipHash algorithm, and comparison between keys is
     * done with {@link Arrays}.equals.
     *
     * @param key The to search for and remove.
     * @throws Exception if an unexpected error occurs during the transactions.
     */
    public void remove(final byte[] key) throws Exception {
        final TransactionResult<Object> result = conn.runTransaction(txn -> {
            populate();
            final int idx = root.bucketIndex(hash(key));
            final Bucket b = Bucket.load(this, refs[idx]);
            final Bucket.ChainMutationResult cmr = b.remove(key);
            if (cmr.done || cmr.chainDelta != 0) {
                if (cmr.b == null) { // must keep old bucket even though it's empty
                    cmr.b.write(true);
                } else if (cmr.b != b) {
                    refs[idx] = cmr.b.objRef;
                }
                if (cmr.done) {
                    root.size--;
                }
                root.bucketCount += cmr.chainDelta;
                write();
            }
            return null;
        });
        if (!result.isSuccessful()) {
            throw result.cause;
        }
    }

    /**
     * Iterate over the entries in the LinearHash. Iteration order is
     * undefined. Also note that as usual, the transaction in which the
     * iteration is occurring may need to restart one or more times in
     * which case the callback may be invoked several times for the same
     * entry. To detect this, call forEach from within a transaction of
     * your own. Iteration will stop as soon as the callback throws an
     * error, which will also abort the transaction.
     *
     * @param action The action to be performed for each entry
     * @throws Exception if an unexpected error occurs during the transactions.
     */
    public void forEach(BiConsumer<? super byte[], ? super GoshawkObjRef> action) throws Exception {
        final TransactionResult<Object> result = conn.runTransaction(txn -> {
            populate();
            for (GoshawkObjRef objRef : refs) {
                final Bucket b = Bucket.load(this, objRef);
                b.forEach(action);
            }
            return null;
        });
        if (!result.isSuccessful()) {
            throw result.cause;
        }
    }

    /**
     * Returns the number of entries in the LinearHash.
     *
     * @return the number of entries in the LinearHash.
     * @throws Exception if an unexpected error occurs during the transactions.
     */
    public int size() throws Exception {
        final TransactionResult<Integer> result = conn.runTransaction(txn -> {
            populate();
            return root.size;
        });
        if (result.isSuccessful()) {
            return result.result;
        } else {
            throw result.cause;
        }
    }

    private void split() {
        final int sOld = root.splitIndex.intValueExact();
        Bucket b = Bucket.load(this, refs[sOld]);
        TransactionResult<GoshawkObjRef> result = conn.runTransaction(txn -> txn.createObject(null));
        if (!result.isSuccessful()) {
            throw new TransactionAbortedException(result.cause);
        }
        final Bucket bNew = Bucket.createEmpty(this, result.result);

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
            bNext = b.next();
            boolean emptied = true;
            for (int idx = 0; idx < b.entries.length; idx++) {
                if (b.isSlotEmpty(idx)) {
                    continue;
                } else if (root.bucketIndex(hash(b.entries[idx])) == sOld) {
                    emptied = false;
                } else {
                    final Bucket.ChainMutationResult cmr = bNew.put(b.entries[idx], b.refs.get(idx + 1));
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
                        b.write(true);
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
                    bPrev.write(true);
                }
                bPrev = b;
            }
        }
        if (bPrev != null) {
            bPrev.write(true);
        }
        bNew.write(true);
    }
}