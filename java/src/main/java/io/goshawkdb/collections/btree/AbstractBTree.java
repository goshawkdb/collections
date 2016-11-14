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
        if (order < 3) {
            throw new IllegalArgumentException("the minimum sensible order is 3");
        }
        this.minNonLeafChildren = ceilHalf(order);
        this.maxNonLeafChildren = order;
        // # leaf keys doesn't actually have to be related to # non-leaf keys
        this.minLeafKeys = this.minNonLeafChildren - 1;
        this.maxLeafKeys = this.maxNonLeafChildren - 1;
        this.root = root;
        this.comparator = comparator;
    }

    static int ceilHalf(int n) {
        return (n / 2) + (n % 2);
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
        final ArrayLike<K> newKeys = node.getKeys().spliceIn(i, key);
        final ArrayLike<V> newVals = node.getValues().spliceIn(i, value);
        final ArrayLike<N> newChildren = child == null ? node.getChildren() : node.getChildren().spliceIn(i, child);
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

    void remove(K key) {
        remove(root, key, true);
        if (root.getChildren().count() == 1) {
            final N child = root.getChildren().first();
            root.update(child.getKeys(), child.getValues(), child.getChildren());
        }
    }

    // returns true if underflow happened
    private boolean remove(N node, K key, boolean isRoot) {
        final Lub lub = findLub(node, key);
        if (node.isLeaf()) {
            if (lub.exact) {
                node.update(node.getKeys().spliceOut(lub.i), node.getValues().spliceOut(lub.i), empty());
                return node.getKeys().count() < minLeafKeys;
            }
            // key wasn't there, but on the plus side, no re-balancing is needed!
            return false;
        }
        final N left = node.getChildren().get(lub.i);
        if (lub.exact) {
            final Pop pop = pop(left);
            node.update(node.getKeys().with(lub.i, pop.key), node.getValues().with(lub.i, pop.value), node.getChildren());
            if (pop.underflow) {
                return fixUnderflow(node, lub.i, isRoot);
            }
            return false;
        }
        final boolean underflow = remove(left, key, false);
        if (underflow) {
            return fixUnderflow(node, lub.i, isRoot);
        }
        return false;
    }

    private boolean fixUnderflow(N node, int i, boolean isRoot) {
        final N child = node.getChildren().get(i);
        if (child.isLeaf() && child.getKeys().count() >= minLeafKeys) {
            throw new IllegalStateException("there was no underflow");
        }
        if (!child.isLeaf() && child.getChildren().count() >= minNonLeafChildren) {
            throw new IllegalStateException("there was no underflow");
        }
        final boolean hasLeftSibling = i > 0;
        if (hasLeftSibling && hasSpare(node.getChildren().get(i - 1))) {
            rotateClockwise(node, i - 1);
            return false;
        }
        final boolean hasRightSibling = i + 1 < node.getChildren().count();
        if (hasRightSibling && hasSpare(node.getChildren().get(i + 1))) {
            rotateAnticlockwise(node, i);
            return false;
        }
        if (hasLeftSibling) {
            return mergeChildren(node, i - 1, isRoot);
        }
        if (hasRightSibling) {
            return mergeChildren(node, i, isRoot);
        }
        if (isRoot) {
            // nothing we can do
            return true;
        }
        throw new IllegalStateException("what, we only had one child?!");
    }

    // child i       k/v i      child i + 1
    //      \          |        /
    //       \         c       /                     b
    //       (... a b)   (d ...)   ------>   (... a)   (c d ...)
    private void rotateClockwise(N node, int i) {
        final N left = node.getChildren().get(i);
        final N right = node.getChildren().get(i + 1);
        final K bKey = left.getKeys().last();
        final V bVal = left.getValues().last();
        final N bChild = left.isLeaf() ? null : left.getChildren().last();
        final K cKey = node.getKeys().get(i);
        final V cVal = node.getValues().get(i);
        left.update(
                left.getKeys().withoutLast(), left.getValues().withoutLast(),
                left.isLeaf() ? empty() : left.getChildren().withoutLast());
        right.update(
                wrap(cKey).concat(right.getKeys()),
                wrap(cVal).concat(right.getValues()),
                right.isLeaf() ? empty() : wrap(bChild).concat(right.getChildren()));
        node.update(
                node.getKeys().with(i, bKey),
                node.getValues().with(i, bVal),
                node.getChildren());
    }

    // child i      k/v i        child i + 1
    //       \        |          /
    //        \       b         /                       c
    //        (... a)   (c d ...)   ------>   (... a b)   (d ...)
    private void rotateAnticlockwise(N node, int i) {
        final N left = node.getChildren().get(i);
        final N right = node.getChildren().get(i + 1);
        final K bKey = node.getKeys().get(i);
        final V bVal = node.getValues().get(i);
        final K cKey = right.getKeys().first();
        final V cVal = right.getValues().first();
        final N cChild = right.isLeaf() ? null : right.getChildren().first();
        left.update(
                left.getKeys().concat(wrap(bKey)),
                left.getValues().concat(wrap(bVal)),
                left.isLeaf() ? empty() : left.getChildren().concat(wrap(cChild)));
        right.update(
                right.getKeys().withoutFirst(), right.getValues().withoutFirst(),
                right.isLeaf() ? empty() : right.getChildren().withoutFirst());
        node.update(
                node.getKeys().with(i, cKey),
                node.getValues().with(i, cVal),
                node.getChildren());
    }

    // merge the i'th key and (i + 1)'st child of node into the i'th child, thereby losing one key and one child
    private boolean mergeChildren(N node, int i, boolean isRoot) {
        final N child = node.getChildren().get(i);
        final N rightSibling = node.getChildren().get(i + 1);
        final K key = node.getKeys().get(i);
        final V value = node.getValues().get(i);
        final ArrayLike<K> newChildKeys = child.getKeys().concat(wrap(key)).concat(rightSibling.getKeys());
        final ArrayLike<V> newChildVals = child.getValues().concat(wrap(value)).concat(rightSibling.getValues());
        final ArrayLike<N> newChildChildren;
        if (child.isLeaf()) {
            newChildChildren = empty();
            checkSizesLeaf(isRoot, newChildKeys, newChildVals, newChildChildren);
        } else {
            newChildChildren = child.getChildren().concat(rightSibling.getChildren());
            checkSizesNonLeaf(isRoot, newChildKeys, newChildVals, newChildChildren);
        }
        child.update(newChildKeys, newChildVals, newChildChildren);
        final ArrayLike<K> newKeys = node.getKeys().spliceOut(i);
        final ArrayLike<V> newVals = node.getValues().spliceOut(i);
        final ArrayLike<N> newChildren = node.getChildren().spliceOut(i + 1);
        if (newVals.count() != newKeys.count()) {
            throw new IllegalStateException("wrong number of values");
        }
        if (newChildren.count() > maxNonLeafChildren) {
            throw new IllegalStateException(String.format("wrong number of children: expected %d to %d, got %d",
                    minNonLeafChildren, maxNonLeafChildren, newChildren.count()));
        }
        if (newChildren.count() != newKeys.count() + 1) {
            throw new IllegalStateException("wrong number of children");
        }
        node.update(newKeys, newVals, newChildren);
        return newChildren.count() < minNonLeafChildren;
    }

    private boolean hasSpare(N n) {
        if (n.isLeaf()) {
            return n.getKeys().count() > minLeafKeys;
        }
        return n.getChildren().count() > minNonLeafChildren;
    }

    private Pop pop(N node) {
        if (node.isLeaf()) {
            final int n = node.getKeys().count() - 1;
            final K key = node.getKeys().get(n);
            final V val = node.getValues().get(n);
            node.update(node.getKeys().sliceTo(n), node.getValues().sliceTo(n), empty());
            return new Pop(key, val, node.getKeys().count() < minLeafKeys);
        }
        final int i = node.getChildren().count() - 1;
        final N lastChild = node.getChildren().get(i);
        final Pop pop = pop(lastChild);
        if (pop.underflow) {
            return new Pop(pop.key, pop.value, fixUnderflow(node, i, false));
        }
        return pop;
    }

    private class Pop {
        final K key;
        final V value;
        final boolean underflow;

        private Pop(K key, V value, boolean underflow) {
            this.key = key;
            this.value = value;
            this.underflow = underflow;
        }
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