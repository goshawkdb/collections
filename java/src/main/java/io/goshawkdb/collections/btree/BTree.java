package io.goshawkdb.collections.btree;

import java.nio.ByteBuffer;
import java.util.function.BiConsumer;

import io.goshawkdb.client.Connection;
import io.goshawkdb.client.GoshawkObjRef;
import io.goshawkdb.client.Transaction;
import io.goshawkdb.client.TransactionResult;

import static io.goshawkdb.collections.btree.ArrayLike.empty;

public class BTree {
    private final Connection conn;
    private final GoshawkObjRef root;

    public BTree(Connection conn, GoshawkObjRef root) {
        this.conn = conn;
        this.root = root;
    }

    private static ByteBuffer packKeys(ArrayLike<byte[]> keys) {
        final MsgPacker packer = new MsgPacker();
        final int n = keys.size();
        packer.packArrayHeader(n);
        for (int i = 0; i < n; i++) {
            packer.writeBinary(keys.get(i));
        }
        return packer.toByteBuffer();
    }

    public static TransactionResult<BTree> createEmpty(final Connection conn) {
        return conn.runTransaction(txn -> new BTree(conn, txn.createObject(packKeys(empty()))));
    }

    public GoshawkObjRef getRoot() {
        return root;
    }

    private NodeImpl toNode(Transaction txn, GoshawkObjRef obj) {
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
        final ArrayLike<NodeImpl> children = refs.sliceFrom(keyCount).map((obj1) -> toNode(txn, obj1));
        final ArrayLike<GoshawkObjRef> values = refs.sliceTo(keyCount);
        return new NodeImpl(txn, obj, ArrayLike.wrap(keys), values, children);
    }

    public TransactionResult<Integer> size() {
        return conn.runTransaction(txn -> tree(txn).size());
    }

    private AbstractBTree<byte[], GoshawkObjRef, NodeImpl> tree(Transaction txn) {
        return new AbstractBTree<>(128, toNode(txn, txn.getObject(root)), Lexicographic.INSTANCE);
    }

    public TransactionResult<Void> put(byte[] key, GoshawkObjRef value) {
        return conn.runTransaction(
                txn -> {
                    tree(txn).put(key, value);
                    return null;
                });
    }

    public TransactionResult<GoshawkObjRef> find(byte[] key) {
        return conn.runTransaction(txn -> tree(txn).find(key));
    }

    public Cursor<byte[], GoshawkObjRef> cursor() throws Exception {
        return conn.runTransaction(txn -> tree(txn).cursor()).getResultOrRethrow();
    }

    public TransactionResult<Void> forEach(BiConsumer<? super byte[], ? super GoshawkObjRef> action) {
        return conn.runTransaction(
                txn -> {
                    tree(txn).forEach(action);
                    return null;
                });
    }

    public TransactionResult<Void> remove(byte[] key) {
        return conn.runTransaction(
                txn -> {
                    tree(txn).remove(key);
                    return null;
                });
    }

    static class NodeImpl implements Node<byte[], GoshawkObjRef, NodeImpl> {
        final Transaction txn;
        final GoshawkObjRef obj;
        ArrayLike<byte[]> keys;
        ArrayLike<GoshawkObjRef> values;
        ArrayLike<NodeImpl> children;

        NodeImpl(
                Transaction txn,
                GoshawkObjRef obj,
                ArrayLike<byte[]> keys,
                ArrayLike<GoshawkObjRef> values,
                ArrayLike<NodeImpl> children) {
            this.txn = txn;
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
            return new NodeImpl(txn, obj, keys, vals, children);
        }
    }
}
