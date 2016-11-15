package io.goshawkdb.collections.btree;

import org.junit.Test;

import java.util.Arrays;

import static io.goshawkdb.collections.btree.ArrayLike.wrap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.quicktheories.quicktheories.QuickTheory.qt;
import static org.quicktheories.quicktheories.generators.SourceDSL.arrays;
import static org.quicktheories.quicktheories.generators.SourceDSL.integers;


public class ArrayLikeTest {
    @Test
    public void testArrayWrapper() {
        qt().forAll(arrays().ofIntegers(integers().all()).withLengthBetween(0, 5))
                .checkAssert(xs -> assertThat(wrap(xs).copyOut(Integer.class)).containsExactly(xs));
    }

    @Test
    public void testConcat() {
        qt().forAll(
                arrays().ofIntegers(integers().all()).withLengthBetween(0, 5),
                arrays().ofIntegers(integers().all()).withLengthBetween(0, 5),
                integers().between(0, 10),
                integers().between(0, 10))
                .assuming((xs, ys, srcPos, length) -> srcPos + length < xs.length + ys.length)
                .checkAssert((xs, ys, srcPos, length) -> {
                    assertCopyToWorks(wrap(xs).concat(wrap(ys)), srcPos, length);
                });
    }

    @Test
    public void testSlice() {
        final ArrayLike<Integer> ints = wrap(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        qt().forAll(
                integers().between(0, 4),
                integers().between(5, 9),
                integers().between(0, 9),
                integers().between(0, 9))
                .assuming((i, j, srcPos, length) -> i <= j && srcPos + length < j - i)
                .checkAssert((i, j, srcPos, length) -> {
                    assertCopyToWorks(new ArrayLike.Slice<>(ints, i, j), srcPos, length);
                });
    }

    private void assertCopyToWorks(ArrayLike<Integer> arr, Integer srcPos, Integer length) {
        final Integer[] copy1 = Arrays.copyOfRange(arr.copyOut(Integer.class), srcPos, srcPos + length);
        final Integer[] copy2 = new Integer[length];
        arr.copyTo(srcPos, copy2, 0, length);
        assertThat(copy1).containsExactly(copy2);
    }
}