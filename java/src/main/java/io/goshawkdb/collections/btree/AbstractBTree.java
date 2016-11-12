package io.goshawkdb.collections.btree;

import java.util.Comparator;
import java.util.function.BiConsumer;

import static io.goshawkdb.collections.btree.ArrayLike.empty;
import static io.goshawkdb.collections.btree.ArrayLike.wrap;

class AbstractBTree<K, V, N extends Node<K, V, N>> {
    private final int minNonLeafChildren;
    private final int maxNonLeafChildren;
    private final int minLeafKeys;
    private final int maxLeafKeys;
    private final N root;
    private final Comparator<K> comparator;

    AbstractBTree(int order, N root, Comparator<K> comparator) {
        this.minNonLeafChildren = (order / 2) + (order % 2);
        this.maxNonLeafChildren = order;
        // # leaf keys doesn't actually have to be related to # non-leaf keys
        this.minLeafKeys = this.minNonLeafChildren - 1;
        this.maxLeafKeys = this.maxNonLeafChildren - 1;
        this.root = root;
        this.comparator = comparator;
    }

    int count() {
        return count(root);
    }

    private int count(N node) {
        return node.getChildren().fold((child, n) -> n + count(child), node.getKeys().count());
    }

    private Lub findLub(final N node, final K key) {
        final int n = node.getKeys().count();
        for (int i = 0; i < n; i++) {
            final int c = comparator.compare(key, node.getKeys().get(i));
            if (c <= 0) {
                return new Lub(i, c == 0);
            }
        }
        return new Lub(n, false);
    }

    private V find(N node, K key) {
        final Lub lub = findLub(node, key);
        if (lub.exact) {
            return node.getValues().get(lub.i);
        }
        if (!node.isLeaf()) {
            return find(node.getChildren().get(lub.i), key);
        }
        return null;
    }

    V find(K key) {
        return find(root, key);
    }

    private Split put(N node, boolean isRoot, K key, V value) {
        final Lub lub = findLub(node, key);
        if (lub.exact) {
            // hooray, we can just replace a ref
            node.update(node.getKeys(), node.getValues().with(lub.i, value), node.getChildren());
            return null;
        }
        if (node.isLeaf()) {
            return putAt(node, isRoot, key, value, null, lub.i);
        }
        final Split split = put(node.getChildren().get(lub.i), false, key, value);
        if (split == null) {
            return null;
        }
        return putAt(node, isRoot, split.key, split.value, split.sibling, lub.i);
    }

    private Split putAt(N node, boolean isRoot, K key, V value, N child, int i) {
        final ArrayLike<K> newKeys = node.getKeys().splice(i, key);
        final ArrayLike<V> newVals = node.getValues().splice(i, value);
        final ArrayLike<N> newChildren = child == null ? node.getChildren() : node.getChildren().splice(i, child);
        if (child == null) {
            // leaf
            if (newKeys.count() > maxLeafKeys) {
                return split(node, newKeys, newVals, null, minLeafKeys);
            }
            checkSizesLeaf(isRoot, newKeys, newVals, newChildren);
        } else {
            if (newChildren.count() > maxNonLeafChildren) {
                return split(node, newKeys, newVals, newChildren, minNonLeafChildren - 1);
            }
            checkSizesNonLeaf(isRoot, newKeys, newVals, newChildren);
        }
        node.update(newKeys, newVals, newChildren);
        return null;
    }

    private Split split(N node, ArrayLike<K> newKeys, ArrayLike<V> newVals, ArrayLike<N> newChildren, int median) {
        final ArrayLike<K> sibKeys = newKeys.sliceTo(median);
        final ArrayLike<K> myKeys = newKeys.sliceFrom(median + 1);
        final ArrayLike<V> sibVals = newVals.sliceTo(median);
        final ArrayLike<V> myVals = newVals.sliceFrom(median + 1);
        final ArrayLike<N> sibChildren, myChildren;
        if (newChildren == null) {
            sibChildren = empty();
            myChildren = empty();
            checkSizesLeaf(false, sibKeys, sibVals, sibChildren);
            checkSizesLeaf(false, myKeys, myVals, myChildren);
        } else {
            sibChildren = newChildren.sliceTo(median + 1);
            myChildren = newChildren.sliceFrom(median + 1);
            checkSizesNonLeaf(false, sibKeys, sibVals, sibChildren);
            checkSizesNonLeaf(false, myKeys, myVals, myChildren);
        }
        final N sib = node.createSibling(sibKeys, sibVals, sibChildren);
        node.update(myKeys, myVals, myChildren);
        return new Split(sib, newKeys.get(median), newVals.get(median));
    }

    private void checkSizesLeaf(boolean isRoot, ArrayLike<?> keys, ArrayLike<?> values, ArrayLike<?> children) {
        if (values.count() != keys.count()) {
            throw new IllegalStateException("wrong number of values");
        }
        if (!isRoot && (keys.count() < minLeafKeys || keys.count() > maxLeafKeys)) {
            throw new IllegalStateException("wrong number of keys");
        }
        if (children.count() != 0) {
            throw new IllegalStateException("wrong number of children");
        }
    }

    private void checkSizesNonLeaf(boolean isRoot, ArrayLike<?> keys, ArrayLike<?> values, ArrayLike<?> children) {
        if (values.count() != keys.count()) {
            throw new IllegalStateException("wrong number of values");
        }
        if (!isRoot && (children.count() < minNonLeafChildren || children.count() > maxNonLeafChildren)) {
            throw new IllegalStateException(String.format("wrong number of children: expected %d to %d, got %d",
                    minNonLeafChildren, maxNonLeafChildren, children.count()));
        }
        if (children.count() != keys.count() + 1) {
            throw new IllegalStateException("wrong number of children");
        }
    }

    void put(K key, V value) {
        final Split split = put(root, true, key, value);
        if (split == null) {
            return;
        }
        final N newOldRoot = root.createSibling(root.getKeys(), root.getValues(), root.getChildren());
        root.update(wrap(split.key), wrap(split.value), wrap(split.sibling, newOldRoot));
    }

    void forEach(BiConsumer<? super K, ? super V> action) {
        forEach(root, action);
    }

    private void forEach(N node, BiConsumer<? super K, ? super V> action) {
        final int n = node.getKeys().count();
        for (int i = 0; i < n; i++) {
            if (!node.isLeaf()) {
                forEach(node.getChildren().get(i), action);
            }
            action.accept(node.getKeys().get(i), node.getValues().get(i));
        }
        if (!node.isLeaf()) {
            forEach(node.getChildren().get(n), action);
        }
    }

    N getRoot() {
        return root;
    }

    private static class Lub {
        final int i;
        final boolean exact;

        Lub(int i, boolean exact) {
            this.i = i;
            this.exact = exact;
        }
    }

    private class Split {
        final N sibling;
        final K key;
        final V value;

        private Split(N sibling, K key, V value) {
            this.sibling = sibling;
            this.key = key;
            this.value = value;
        }
    }
}