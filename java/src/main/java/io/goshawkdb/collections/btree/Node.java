package io.goshawkdb.collections.btree;

interface Node<K, V, Self extends Node<K, V, Self>> {
    ArrayLike<K> getKeys();

    ArrayLike<Self> getChildren();

    ArrayLike<V> getValues();

    default boolean isLeaf() {
        return getChildren().count() == 0;
    }

    void update(ArrayLike<K> newKeys, ArrayLike<V> newVals, ArrayLike<Self> newChildren);

    Self createSibling(ArrayLike<K> newKeys, ArrayLike<V> newVals, ArrayLike<Self> newChildren);
}