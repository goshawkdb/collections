package io.goshawkdb.collections.btree;

import io.goshawkdb.client.GoshawkObjRef;
import io.goshawkdb.client.Transaction;

import java.nio.ByteBuffer;
import java.util.function.BiConsumer;

import static io.goshawkdb.collections.btree.ArrayLike.empty;

public class BTree {
    private final Transaction txn;
    private final GoshawkObjRef root;

    public BTree(Transaction txn, GoshawkObjRef root) {
        this.txn = txn;
        this.root = root;
    }

    private static ByteBuffer packKeys(ArrayLike<byte[]> keys) {
        final MsgPacker packer = new MsgPacker();
        final int n = keys.count();
        packer.packArrayHeader(n);
        for (int i = 0; i < n; i++) {
            packer.writeBinary(keys.get(i));
        }
        return packer.toByteBuffer();
    }

    public static BTree createEmpty(final Transaction txn) {
        return new BTree(txn, txn.createObject(packKeys(empty())));
    }

    public GoshawkObjRef getRoot() {
        return root;
    }

    private NodeImpl toNode(GoshawkObjRef obj) {
        final MsgUnpacker unpacker = new MsgUnpacker(obj.getValue());
        final int keyCount = unpacker.unpackArrayHeader();
        final byte[][] keys = new byte[keyCount][];
        for (int i = 0; i < keyCount; i++) {
            keys[i] = unpacker.readBinary();
        }
        if (unpacker.hasNext()) {
            throw new IllegalStateException("trailing garbage in leaf");
        }
        final ArrayLike<GoshawkObjRef> refs = ArrayLike.wrap(obj.getReferences());
        final ArrayLike<NodeImpl> children = refs.sliceFrom(keyCount).map(this::toNode);
        final ArrayLike<GoshawkObjRef> values = refs.sliceTo(keyCount);
        return new NodeImpl(obj, ArrayLike.wrap(keys), values, children);
    }

    public int count() {
        return tree().count();
    }

    private AbstractBTree<byte[], GoshawkObjRef, NodeImpl> tree() {
        return new AbstractBTree<>(3, toNode(root), Lexicographic.INSTANCE);
    }

    public void put(byte[] key, GoshawkObjRef value) {
        tree().put(key, value);
    }

    public GoshawkObjRef find(byte[] key) {
        return tree().find(key);
    }

    public void forEach(BiConsumer<? super byte[], ? super GoshawkObjRef> action) {
        tree().forEach(action);
    }

    class NodeImpl implements Node<byte[], GoshawkObjRef, NodeImpl> {
        final GoshawkObjRef obj;
        ArrayLike<byte[]> keys;
        ArrayLike<GoshawkObjRef> values;
        ArrayLike<NodeImpl> children;

        NodeImpl(GoshawkObjRef obj, ArrayLike<byte[]> keys, ArrayLike<GoshawkObjRef> values, ArrayLike<NodeImpl> children) {
            this.obj = obj;
            this.keys = keys;
            this.values = values;
            this.children = children;
        }

        @Override
        public ArrayLike<byte[]> getKeys() {
            return keys;
        }

        @Override
        public ArrayLike<NodeImpl> getChildren() {
            return children;
        }

        @Override
        public ArrayLike<GoshawkObjRef> getValues() {
            return values;
        }

        @Override
        public void update(ArrayLike<byte[]> newKeys, ArrayLike<GoshawkObjRef> newVals, ArrayLike<NodeImpl> newChildren) {
            final ArrayLike<GoshawkObjRef> refs = newVals.concat(newChildren.map(NodeImpl::getObj));
            obj.set(packKeys(newKeys), refs.copyOut(GoshawkObjRef.class));
            keys = newKeys;
            values = newVals;
            children = newChildren;
        }

        private GoshawkObjRef getObj() {
            return obj;
        }

        @Override
        public NodeImpl createSibling(ArrayLike<byte[]> keys, ArrayLike<GoshawkObjRef> vals, ArrayLike<NodeImpl> children) {
            final ArrayLike<GoshawkObjRef> refs = vals.concat(children.map(NodeImpl::getObj));
            final GoshawkObjRef obj = txn.createObject(packKeys(keys), refs.copyOut(GoshawkObjRef.class));
            return new NodeImpl(obj, keys, vals, children);
        }
    }
}