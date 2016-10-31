package io.goshawkdb.collections.linearhash;

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.BiConsumer;

import io.goshawkdb.client.GoshawkObjRef;
import io.goshawkdb.client.TransactionAbortedException;
import io.goshawkdb.client.TransactionResult;

import static io.goshawkdb.collections.linearhash.Root.BucketCapacity;

final class Bucket {
    private final LinearHash lh;

    GoshawkObjRef objRef;
    byte[][] entries;
    final ArrayList<GoshawkObjRef> refs = new ArrayList<>();

    private ByteBuffer value;

    private Bucket(final LinearHash lhash, final GoshawkObjRef ref, final boolean populate) {
        lh = lhash;
        objRef = ref;
        if (populate) {
            this.populate();
        }
    }

    static Bucket load(final LinearHash lhash, final GoshawkObjRef ref) {
        return new Bucket(lhash, ref, true);
    }

    static Bucket createEmpty(final LinearHash lhash, final GoshawkObjRef ref) {
        final Bucket b = new Bucket(lhash, ref, false);
        b.entries = new byte[BucketCapacity][];
        b.refs.add(ref);
        return b;
    }

    private void populate() {
        TransactionResult<Object> result = lh.conn.runTransaction(txn -> {
            objRef = txn.getObject(objRef);
            value = objRef.getValue();
            refs.clear();
            Collections.addAll(refs, objRef.getReferences());
            try (final MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(value)) {
                while (unpacker.hasNext()) {
                    MessageFormat f = unpacker.getNextFormat();
                    if (!(f == MessageFormat.FIXARRAY || f == MessageFormat.ARRAY16 || f == MessageFormat.ARRAY32)) {
                        throw new IllegalArgumentException("value does not contain a LinearHash bucket");
                    }
                    entries = new byte[unpacker.unpackArrayHeader()][];
                    for (int idx = 0; idx < entries.length; idx++) {
                        final int keyLen = unpacker.unpackBinaryHeader();
                        if (keyLen > 0) {
                            final byte[] entry = new byte[keyLen];
                            unpacker.readPayload(entry);
                            entries[idx] = entry;
                        }
                    }
                }
            } catch (Exception e) {
                throw new TransactionAbortedException(e);
            }
            return null;
        });
        if (!result.isSuccessful()) {
            entries = null;
            value = null;
            refs.clear();
            throw new TransactionAbortedException(result.cause);
        }
    }

    void write(boolean updateEntries) {
        if (updateEntries) {
            try (final MessageBufferPacker packer = MessagePack.newDefaultBufferPacker()) {
                packer.packArrayHeader(entries.length);
                for (byte[] entry : entries) {
                    if (entry == null) {
                        packer.packBinaryHeader(0);
                    } else {
                        packer.packBinaryHeader(entry.length);
                        packer.writePayload(entry);
                    }
                }
                value = ByteBuffer.wrap(packer.toByteArray());
            } catch (Exception e) {
                throw new TransactionAbortedException(e);
            }
        }
        objRef.set(value, refs.toArray(new GoshawkObjRef[refs.size()]));
    }

    GoshawkObjRef find(final byte[] key) {
        for (int idx = 0; idx < entries.length; idx++) {
            if (isSlotEmpty(idx)) {
                continue;
            } else if (Arrays.equals(key, entries[idx])) {
                return refs.get(idx + 1);
            }
        }
        final Bucket b = next();
        if (b == null) {
            return null;
        } else {
            return b.find(key);
        }
    }

    ChainMutationResult put(final byte[] key, final GoshawkObjRef value) {
        int slot = -1;
        for (int idx = 0; idx < entries.length; idx++) {
            if (isSlotEmpty(idx)) {
                if (slot == -1) {
                    // we've found a hole for it, let's use it. But we can
                    // only use it if we're sure it's not already in this
                    // bucket.
                    slot = idx;
                }
            } else if (Arrays.equals(key, entries[idx])) {
                refs.set(idx + 1, value);
                // we didn't change any keys so don't need to serialize
                write(false);
                return new ChainMutationResult(this, false, 0);
            }
        }
        if (slot == -1) {
            return putInNext(key, value);
        } else {
            return putInSlot(key, value, slot);
        }
    }

    private ChainMutationResult putInSlot(final byte[] key, final GoshawkObjRef value, final int slot) {
        entries[slot] = key;
        final int refSlot = slot + 1;
        if (refSlot == refs.size()) {
            refs.add(value);
        } else {
            refs.set(refSlot, value);
        }

        final Bucket b = next();
        if (b == null) {
            write(true);
            return new ChainMutationResult(this, true, 0);

        } else {
            final ChainMutationResult cmr = b.remove(key);
            if (cmr.b == null) {
                refs.set(0, objRef);
            } else {
                refs.set(0, cmr.b.objRef);
            }
            write(true);
            return new ChainMutationResult(this, !cmr.done, cmr.chainDelta);
        }
    }

    private ChainMutationResult putInNext(final byte[] key, final GoshawkObjRef value) {
        Bucket b = next();
        if (b == null) {
            TransactionResult<GoshawkObjRef> result = lh.conn.runTransaction(txn -> txn.createObject(null));
            if (!result.isSuccessful()) {
                throw new TransactionAbortedException(result.cause);
            }
            b = createEmpty(lh, result.result);
            final ChainMutationResult cmr = b.put(key, value);
            refs.set(0, cmr.b.objRef);
            // we didn't change any keys so don't need to serialize
            write(false);
            return new ChainMutationResult(this, cmr.done, cmr.chainDelta + 1);

        } else {
            // next cannot change here
            final ChainMutationResult cmr = b.put(key, value);
            return new ChainMutationResult(this, cmr.done, cmr.chainDelta);
        }
    }

    ChainMutationResult remove(final byte[] key) {
        int slot = -1;
        for (int idx = 0; idx < entries.length; idx++) {
            if (isSlotEmpty(idx)) {
                continue;
            } else if (Arrays.equals(key, entries[idx])) {
                slot = idx;
                break;
            }
        }

        if (slot == -1) {
            Bucket b = next();
            if (b == null) {
                return new ChainMutationResult(this, false, 0);
            } else {
                final ChainMutationResult cmr = b.remove(key);
                if (cmr.b == null) {
                    refs.set(0, objRef);
                    write(false);
                } else if (!refs.get(0).referencesSameAs(cmr.b.objRef)) {
                    refs.set(0, cmr.b.objRef);
                    b.write(false);
                }
                return new ChainMutationResult(this, cmr.done, cmr.chainDelta);
            }

        } else {
            entries[slot] = null;
            final int refSlot = slot + 1;
            refs.set(refSlot, objRef);
            tidyRefTail();
            if (refs.size() == 1) { // we're empty; don't need to write us, just disconnect us.
                return new ChainMutationResult(next(), true, -1);
            } else {
                write(true);
                return new ChainMutationResult(this, true, 0);
            }
        }
    }

    void forEach(BiConsumer<? super byte[], ? super GoshawkObjRef> action) {
        for (int idx = 0; idx < entries.length; idx++) {
            if (isSlotEmpty(idx)) {
                continue;
            }
            action.accept(entries[idx], refs.get(idx + 1));
        }
        final Bucket b = next();
        if (b != null) {
            b.forEach(action);
        }
    }

    void tidyRefTail() {
        for (int idx = refs.size() - 1; idx > 0 && objRef.referencesSameAs(refs.get(idx)); idx--) {
            refs.remove(idx);
        }
    }

    boolean isSlotEmpty(final int idx) {
        return idx + 1 >= refs.size() || refs.get(idx + 1).referencesSameAs(objRef);
    }

    Bucket next() {
        final GoshawkObjRef ref = refs.get(0);
        if (ref.referencesSameAs(objRef)) {
            return null;
        } else {
            return load(lh, ref);
        }
    }

    static class ChainMutationResult {
        final Bucket b;
        final boolean done;
        final int chainDelta;

        ChainMutationResult(final Bucket b, final boolean done, final int chainDelta) {
            this.b = b;
            this.done = done;
            this.chainDelta = chainDelta;
        }
    }
}
