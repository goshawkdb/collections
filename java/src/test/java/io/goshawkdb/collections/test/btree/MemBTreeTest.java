package io.goshawkdb.collections.test.btree;

import io.goshawkdb.collections.btree.MemBTree;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class MemBTreeTest {
    @Test
    public void bruteForceTest() throws IOException {
        for (int n = 2; n < 9; n++) {
            for (int order = 2; order <= n + 1; order++) {
                bruteForceTest(n, order);
            }
        }
    }

    private void bruteForceTest(int n, int order) {
        forEachPerm(n, p -> {
            final MemBTree t = new MemBTree(order);
            for (int i = 0; i < n; i++) {
                t.put(i, i);
                t.checkInvariants();
            }
            assertThat(t.count(), equalTo(n));
            for (int i = 0; i < n; i++) {
                assertThat(t.find(i), equalTo(i));
            }
        });
    }

    // need to convince myself the test itself works ;)
    @Test
    public void testForEachPerm() {
        final List<int[]> perms = new LinkedList<>();
        forEachPerm(3, p -> perms.add(Arrays.copyOf(p, p.length)));
        assertThat(perms.size(), equalTo(6));
        assertThat(perms.get(0), equalTo(new int[]{0, 1, 2}));
        assertThat(perms.get(1), equalTo(new int[]{0, 2, 1}));
        assertThat(perms.get(2), equalTo(new int[]{1, 0, 2}));
        assertThat(perms.get(3), equalTo(new int[]{1, 2, 0}));
        assertThat(perms.get(4), equalTo(new int[]{2, 0, 1}));
        assertThat(perms.get(5), equalTo(new int[]{2, 1, 0}));
    }

    private void forEachPerm(int n, Consumer<int[]> f) {
        final int[] xs = new int[n];
        for (int i = 0; i < n; i++) {
            xs[i] = i;
        }
        while (true) {
            f.accept(xs);
            int i = n - 1;
            while (i >= 1 && xs[i - 1] >= xs[i]) {
                i--;
            }
            if (i == 0) {
                return;
            }
            int j = n - 1;
            while (j >= 0 && xs[i - 1] >= xs[j]) {
                j--;
            }
            swap(xs, i - 1, j);
            int k = n - 1;
            while (i < k) {
                swap(xs, i, k);
                i++;
                k--;
            }
        }
    }

    private static void swap(int[] xs, int i, int j) {
        final int x = xs[i];
        xs[i] = xs[j];
        xs[j] = x;
    }
}