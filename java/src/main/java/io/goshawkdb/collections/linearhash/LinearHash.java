package io.goshawkdb.collections.linearhash;

import com.zackehh.siphash.SipHash;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;

import io.goshawkdb.client.Connection;
import io.goshawkdb.client.GoshawkObjRef;
import io.goshawkdb.client.TransactionAbortedException;
import io.goshawkdb.client.TransactionResult;

public class LinearHash {
    public final Connection conn;
    public GoshawkObjRef objRef;

    private Root root;
    private GoshawkObjRef[] refs;
    private SipHash sipHash;

    public LinearHash(final Connection c, final GoshawkObjRef ref) {
        conn = c;
        objRef = ref;
    }

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