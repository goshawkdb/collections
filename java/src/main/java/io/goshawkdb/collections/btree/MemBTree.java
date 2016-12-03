package io.goshawkdb.collections.btree;

import java.util.Comparator;
import java.util.function.BiConsumer;

import static io.goshawkdb.collections.btree.AbstractBTree.ceilHalf;
import static io.goshawkdb.collections.btree.ArrayLike.empty;
import static io.goshawkdb.collections.btree.ArrayLike.wrap;
import static io.goshawkdb.collections.btree.ArrayLike.wrapArray;

public class MemBTree<K> {
    private final int order;
    private final Comparator<K> comparator;
    private final AbstractBTree<K, Object, NodeImpl<K>> tree;

    public MemBTree(int order, Comparator<K> comparator) {
        this(order, comparator, new NodeImpl<>(empty(), empty(), empty()));
    }

    private MemBTree(int order, Comparator<K> comparator, NodeImpl<K> root) {
        this.order = order;
        this.comparator = comparator;
        this.tree = new AbstractBTree<>(order, root, comparator);
    }

    public int size() {
        return tree.size();
    }

    public void put(K key, Object value) {
        tree.put(key, value);
    }

    public Object find(K key) {
        return tree.find(key);
    }

    public void forEach(BiConsumer<? super K, ? super Object> action) {
        tree.forEach(action);
    }

    public String sketch() {
        final StringBuilder b = new StringBuilder();
        sketch(tree.getRoot(), b);
        return b.toString();
    }

    static <K> void sketch(NodeImpl<K> node, StringBuilder b) {
        b.append('(');
        final int n = node.getKeys().size();
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
        tree.getRoot().checkInvariants(comparator);
    }

    public void remove(K key) {
        tree.remove(key);
    }

    public static void allTrees(int order, int height, int firstKey, BiConsumer<MemBTree<Integer>, Integer> f) {
        if (height < 1) {
            throw new IllegalArgumentException();
        }
        if (height == 1) {
            for (int n = ceilHalf(order) - 1; n <= order - 1; n++) {
                final Integer[] keys = new Integer[n];
                for (int i = 0; i < keys.length; i++) {
                    keys[i] = firstKey + i;
                }
                f.accept(
                        new MemBTree<>(
                                order, Comparator.naturalOrder(), new NodeImpl<>(wrapArray(keys), wrapArray(keys), empty())),
                        firstKey + n);
            }
            return;
        }
        allTrees1(
                order,
                height - 1,
                firstKey,
                order,
                (ts, is) -> {
                    final NodeImpl<Integer> node = new NodeImpl<>(is.withoutLast(), is.withoutLast().map(i -> (Object) i), ts);
                    f.accept(new MemBTree<>(order, Comparator.naturalOrder(), node), is.last());
                });
    }

    private static void allTrees1(
            int order, int height, int firstKey, int n, BiConsumer<ArrayLike<NodeImpl<Integer>>, ArrayLike<Integer>> f) {
        if (n < 1) {
            throw new IllegalArgumentException();
        }
        allTrees(
                order,
                height,
                firstKey,
                (t, i) -> {
                    final NodeImpl<Integer> node = t.getRoot();
                    if (n == 1) {
                        f.accept(wrap(node), wrap(i));
                        return;
                    }
                    allTrees1(
                            order,
                            height,
                            i + 1,
                            n - 1,
                            (nodes, is) -> f.accept(wrap(node).concat(nodes), wrap(i).concat(is)));
                });
    }

    private NodeImpl<K> getRoot() {
        return tree.getRoot();
    }

    public MemBTree<K> copy() {
        return new MemBTree<>(order, comparator, getRoot().copy());
    }

    public Cursor<K, Object> cursor() {
        return tree.cursor();
    }

    public Cursor<K, Object> cursor(K k) {
        return tree.cursor(k);
    }

    static class NodeImpl<K> implements Node<K, Object, NodeImpl<K>> {
        ArrayLike<K> keys;
        ArrayLike<Object> values;
        ArrayLike<NodeImpl<K>> children;

        NodeImpl(ArrayLike<K> keys, ArrayLike<Object> values, ArrayLike<NodeImpl<K>> children) {
            this.keys = keys;
            this.values = values;
            this.children = children;
        }

        @Override
        public ArrayLike<K> getKeys() {
            return keys;
        }

        @Override
        public ArrayLike<NodeImpl<K>> getChildren() {
            return children;
        }

        @Override
        public ArrayLike<Object> getValues() {
            return values;
        }

        @Override
        public void update(ArrayLike<K> newKeys, ArrayLike<Object> newVals, ArrayLike<NodeImpl<K>> newChildren) {
            keys = newKeys.copy();
            values = newVals.copy();
            children = newChildren.copy();
        }

        @Override
        public NodeImpl<K> createSibling(ArrayLike<K> keys, ArrayLike<Object> vals, ArrayLike<NodeImpl<K>> children) {
            return new NodeImpl<>(keys, vals, children);
        }

        void checkInvariants(Comparator<K> comparator) {
            checkLeafDepth();
            checkKeyOrder(comparator, null, null);
        }

        void checkKeyOrder(Comparator<K> comparator, K lb, K ub) {
            final int n = keys.size();
            for (int i = 0; i < n; i++) {
                if ((lb != null && comparator.compare(keys.get(i), lb) < 0)
                        || (ub != null && comparator.compare(keys.get(i), ub) >= 0)) {
                    throw new IllegalStateException("wrong order");
                }
            }
            if (!isLeaf()) {
                for (int i = 0; i <= n; i++) {
                    children.get(i).checkKeyOrder(comparator, i > 0 ? keys.get(i - 1) : null, i < n ? keys.get(i) : null);
                }
            }
        }

        int checkLeafDepth() {
            if (isLeaf()) {
                return 0;
            }
            final ArrayLike<NodeImpl<K>> children = getChildren();
            final int depth = children.get(0).checkLeafDepth();
            final int n = children.size();
            for (int i = 1; i < n; i++) {
                if (children.get(i).checkLeafDepth() != depth) {
                    throw new IllegalStateException("not all leaves are at the same depth");
                }
            }
            return depth + 1;
        }

        NodeImpl<K> copy() {
            return new NodeImpl<>(keys.copy(), values.copy(), children.map(NodeImpl::copy).copy());
        }
    }
}
