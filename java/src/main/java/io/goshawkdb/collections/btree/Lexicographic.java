package io.goshawkdb.collections.btree;

import java.util.Comparator;

public enum Lexicographic implements Comparator<byte[]> {
    INSTANCE;

    public int compare(byte[] a, byte[] b) {
        for (int i = 0; i < a.length && i < b.length; i++) {
            final int c = Integer.compare(a[i], b[i]);
            if (c != 0) {
                return c;
            }
        }
        return Integer.compare(a.length, b.length);
    }
}
