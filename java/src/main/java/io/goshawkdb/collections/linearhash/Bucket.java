package io.goshawkdb.collections.linearhash;

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.BiConsumer;

import io.goshawkdb.client.RefCap;
import io.goshawkdb.client.Transaction;
import io.goshawkdb.client.ValueRefs;

import static io.goshawkdb.collections.linearhash.Root.BucketCapacity;

final class Bucket {
    private final LinearHash lh;

    RefCap objRef;
    byte[][] entries;
    final ArrayList<RefCap> refs = new ArrayList<>();

    private ByteBuffer value;

    private Bucket(final Transaction txn, final LinearHash lhash, final RefCap ref, final boolean populate) {
        lh = lhash;
        objRef = ref;
        if (populate) {
            this.populate(txn);
        }
    }

    static Bucket load(final Transaction txn, final LinearHash lhash, final RefCap ref) {
        return new Bucket(txn, lhash, ref, true);
    }

    static Bucket createEmpty(final LinearHash lhash, final RefCap ref) {
        final Bucket b = new Bucket(null, lhash, ref, false);
        b.entries = new byte[BucketCapacity][];
        b.refs.add(ref);
        return b;
    }

    private void populate(final Transaction txn) {
        final ValueRefs objVR = txn.read(objRef);
        if (txn.restartNeeded()) {
            return;
        }
        refs.clear();
        Collections.addAll(refs, objVR.references);
        value = objVR.value;
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
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    void write(final Transaction txn, final boolean updateEntries) {
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
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        txn.write(objRef, value, refs.toArray(new RefCap[refs.size()]));
    }

    RefCap find(final Transaction txn, final byte[] key) {
        for (int idx = 0; idx < entries.length; idx++) {
            if (isSlotEmpty(idx)) {
                continue;
            } else if (Arrays.equals(key, entries[idx])) {
                return refs.get(idx + 1);
            }
        }
        final Bucket b = next(txn);
        if (b == null || txn.restartNeeded()) {
            return null;
        } else {
            return b.find(txn, key);
        }
    }

    ChainMutationResult put(final Transaction txn, final byte[] key, final RefCap value) {
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
                write(txn, false);
                if (txn.restartNeeded()) {
                    return null;
                }
                return new ChainMutationResult(this, false, 0);
            }
        }
        if (slot == -1) {
            return putInNext(txn, key, value);
        } else {
            return putInSlot(txn, key, value, slot);
        }
    }

    private ChainMutationResult putInSlot(final Transaction txn, final byte[] key, final RefCap value, final int slot) {
        entries[slot] = key;
        final int refSlot = slot + 1;
        if (refSlot == refs.size()) {
            refs.add(value);
        } else {
            refs.set(refSlot, value);
        }

        final Bucket b = next(txn);
        if (txn.restartNeeded()) {
            return null;
        } else if (b == null) {
            write(txn, true);
            if (txn.restartNeeded()) {
                return null;
            }
            return new ChainMutationResult(this, true, 0);

        } else {
            final ChainMutationResult cmr = b.remove(txn, key);
            if (txn.restartNeeded()) {
                return null;
            } else if (cmr.b == null) {
                refs.set(0, objRef);
            } else {
                refs.set(0, cmr.b.objRef);
            }
            write(txn, true);
            if (txn.restartNeeded()) {
                return null;
            }
            return new ChainMutationResult(this, !cmr.done, cmr.chainDelta);
        }
    }

    private ChainMutationResult putInNext(final Transaction txn, final byte[] key, final RefCap value) {
        Bucket b = next(txn);
        if (txn.restartNeeded()) {
            return null;
        } else if (b == null) {
            b = createEmpty(lh, txn.create(null));
            if (txn.restartNeeded()) {
                return null;
            }
            final ChainMutationResult cmr = b.put(txn, key, value);
            if (txn.restartNeeded()) {
                return null;
            }
            refs.set(0, cmr.b.objRef);
            // we didn't change any keys so don't need to serialize
            write(txn, false);
            if (txn.restartNeeded()) {
                return null;
            }
            return new ChainMutationResult(this, cmr.done, cmr.chainDelta + 1);

        } else {
            // next cannot change here
            final ChainMutationResult cmr = b.put(txn, key, value);
            if (txn.restartNeeded()) {
                return null;
            }
            return new ChainMutationResult(this, cmr.done, cmr.chainDelta);
        }
    }

    ChainMutationResult remove(final Transaction txn, final byte[] key) {
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
            Bucket b = next(txn);
            if (txn.restartNeeded()) {
                return null;
            } else if (b == null) {
                return new ChainMutationResult(this, false, 0);
            } else {
                final ChainMutationResult cmr = b.remove(txn, key);
                if (txn.restartNeeded()) {
                    return null;
                } else if (cmr.b == null) {
                    refs.set(0, objRef);
                    write(txn, false);
                    if (txn.restartNeeded()) {
                        return null;
                    }
                } else if (!refs.get(0).sameReferent(cmr.b.objRef)) {
                    refs.set(0, cmr.b.objRef);
                    b.write(txn, false);
                    if (txn.restartNeeded()) {
                        return null;
                    }
                }
                return new ChainMutationResult(this, cmr.done, cmr.chainDelta);
            }

        } else {
            entries[slot] = null;
            final int refSlot = slot + 1;
            refs.set(refSlot, objRef);
            tidyRefTail();
            if (refs.size() == 1) { // we're empty; don't need to write us, just disconnect us.
                final Bucket next = next(txn);
                if (txn.restartNeeded()) {
                    return null;
                }
                return new ChainMutationResult(next, true, -1);
            } else {
                write(txn, true);
                if (txn.restartNeeded()) {
                    return null;
                }
                return new ChainMutationResult(this, true, 0);
            }
        }
    }

    void forEach(final Transaction txn, BiConsumer<? super byte[], ? super RefCap> action) {
        for (int idx = 0; idx < entries.length; idx++) {
            if (isSlotEmpty(idx)) {
                continue;
            }
            action.accept(entries[idx], refs.get(idx + 1));
        }
        final Bucket b = next(txn);
        if (txn.restartNeeded()) {
            return;
        } else if (b != null) {
            b.forEach(txn, action);
        }
    }

    void tidyRefTail() {
        for (int idx = refs.size() - 1; idx > 0 && objRef.sameReferent(refs.get(idx)); idx--) {
            refs.remove(idx);
        }
    }

    boolean isSlotEmpty(final int idx) {
        return idx + 1 >= refs.size() || refs.get(idx + 1).sameReferent(objRef);
    }

    Bucket next(final Transaction txn) {
        final RefCap ref = refs.get(0);
        if (ref.sameReferent(objRef)) {
            return null;
        } else {
            return load(txn, lh, ref);
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
