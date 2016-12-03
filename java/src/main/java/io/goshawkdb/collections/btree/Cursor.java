package io.goshawkdb.collections.btree;

import java.util.Stack;

public class Cursor<K, V> {
    private final Stack<Frame<K, V, ?>> stack;

    Cursor(Stack<Frame<K, V, ?>> stack) {
        this.stack = stack;
    }

    public K getKey() {
        if (stack.isEmpty()) {
            return null;
        } else {
            return stack.peek().getKey();
        }
    }

    public V getValue() {
        if (stack.isEmpty()) {
            return null;
        } else {
            return stack.peek().getValue();
        }
    }

    public void moveRight() {
        final Frame<K, V, ?> f = stack.peek();
        if (f.canMoveRight()) {
            f.moveRight();
            while (stack.peek().canMoveDown()) {
                stack.push(stack.peek().getFrameBelow());
            }
        } else {
            while (!stack.isEmpty() && !stack.peek().canMoveRight()) {
                stack.pop();
            }
        }
    }

    public boolean inTree() {
        return !stack.isEmpty();
    }

    static class Frame<K, V, N extends Node<K, V, N>> {
        final N node;
        int i;

        Frame(N node, int i) {
            this.node = node;
            this.i = i;
        }

        public K getKey() {
            return this.node.getKeys().get(i);
        }

        public V getValue() {
            return this.node.getValues().get(i);
        }

        boolean canMoveRight() {
            return i < (node.isLeaf() ? node.getKeys() : node.getChildren()).size() - 1;
        }

        void moveRight() {
            i++;
        }

        boolean canMoveDown() {
            return !node.isLeaf();
        }

        Frame<K, V, N> getFrameBelow() {
            return new Frame<>(node.getChildren().get(i), 0);
        }
    }
}
