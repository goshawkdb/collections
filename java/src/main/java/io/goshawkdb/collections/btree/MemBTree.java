package io.goshawkdb.collections.btree;

import java.util.Comparator;
import java.util.function.BiConsumer;

import static io.goshawkdb.collections.btree.ArrayLike.empty;

public class MemBTree {
    private final AbstractBTree<Integer, Object, NodeImpl> tree;

    public MemBTree(int order) {
        this.tree = new AbstractBTree<>(order, new NodeImpl(empty(), empty(), empty()), Comparator.naturalOrder());
    }

    public int count() {
        return tree.count();
    }

    public void put(Integer key, Object value) {
        tree.put(key, value);
    }

    public Object find(Integer key) {
        return tree.find(key);
    }

    public void forEach(BiConsumer<? super Integer, ? super Object> action) {
        tree.forEach(action);
    }

    String sketch() {
        final StringBuilder b = new StringBuilder();
        sketch(tree.getRoot(), b);
        return b.toString();
    }

    private void sketch(NodeImpl node, StringBuilder b) {
        b.append('(');
        final int n = node.getKeys().count();
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                b.append(' ');
            }
            if (!node.isLeaf()) {
                sketch(node.getChildren().get(i), b);
                b.append(' ');
            }
            b.append(node.getKeys().get(i).toString());
        }
        if (!node.isLeaf()) {
            b.append(' ');
            sketch(node.getChildren().get(n), b);
        }
        b.append(')');
    }

    public void checkInvariants() {
        tree.getRoot().checkInvariants();
    }

    class NodeImpl implements Node<Integer, Object, NodeImpl> {
        ArrayLike<Integer> keys;
        ArrayLike<Object> values;
        ArrayLike<NodeImpl> children;

        NodeImpl(ArrayLike<Integer> keys, ArrayLike<Object> values, ArrayLike<NodeImpl> children) {
            this.keys = keys;
            this.values = values;
            this.children = children;
        }

        @Override
        public ArrayLike<Integer> getKeys() {
            return keys;
        }

        @Override
        public ArrayLike<NodeImpl> getChildren() {
            return children;
        }

        @Override
        public ArrayLike<Object> getValues() {
            return values;
        }

        @Override
        public void update(ArrayLike<Integer> newKeys, ArrayLike<Object> newVals, ArrayLike<NodeImpl> newChildren) {
            keys = newKeys;
            values = newVals;
            children = newChildren;
        }

        @Override
        public NodeImpl createSibling(ArrayLike<Integer> keys, ArrayLike<Object> vals, ArrayLike<NodeImpl> children) {
            return new NodeImpl(keys, vals, children);
        }

        void checkInvariants() {
            checkLeafDepth();
            checkKeyOrder(null, null);
        }

        void checkKeyOrder(Integer lb, Integer ub) {
            final int n = keys.count();
            for (int i = 0; i < n; i++) {
                if ((lb != null && keys.get(i) < lb) || (ub != null && keys.get(i) >= ub)) {
                    throw new IllegalStateException("wrong order");
                }
            }
            if (!isLeaf()) {
                for (int i = 0; i <= n; i++) {
                    children.get(i).checkKeyOrder(
                            i > 0 ? keys.get(i - 1) : null,
                            i < n ? keys.get(i) : null);
                }
            }
        }

        int checkLeafDepth() {
            if (isLeaf()) {
                return 0;
            }
            final ArrayLike<NodeImpl> children = getChildren();
            final int depth = children.get(0).checkLeafDepth();
            final int n = children.count();
            for (int i = 1; i < n; i++) {
                if (children.get(i).checkLeafDepth() != depth) {
                    throw new IllegalStateException("not all leaves are at the same depth");
                }
            }
            return depth + 1;
        }
    }
}