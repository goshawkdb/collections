package io.goshawkdb.collections.test.btree;

import static io.goshawkdb.collections.btree.ArrayLike.wrap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.quicktheories.quicktheories.QuickTheory.qt;
import static org.quicktheories.quicktheories.generators.SourceDSL.arrays;
import static org.quicktheories.quicktheories.generators.SourceDSL.integers;

import io.goshawkdb.collections.btree.ArrayLike;
import java.util.Arrays;
import org.junit.Test;

public class ArrayLikeTest {
    @Test
    public void testArrayWrapper() {
        qt().forAll(arrays().ofIntegers(integers().all()).withLengthBetween(0, 5))
                .checkAssert(xs -> assertThat(wrap(xs).copyOut(Integer.class)).containsExactly(xs));
    }

    @Test
    public void testConcat() {
        final ArrayLike<Integer> concat = wrap(0, 1, 2).concat(wrap(3, 4, 5));
        final Integer[] dst = new Integer[6];
        for (int srcPos = 0; srcPos < 6; srcPos++) {
            for (int length = 0; length < 6; length++) {
                if (srcPos + length <= 6) {
                    assertCopyToWorks(concat, srcPos, length);
                } else {
                    final int srcPos1 = srcPos;
                    final int length1 = length;
                    assertThatThrownBy(() -> concat.copyTo(srcPos1, dst, 0, length1))
                            .isInstanceOf(IndexOutOfBoundsException.class);
                }
            }
        }
    }

    @Test
    public void testSlice() {
        final ArrayLike<Integer> ints = wrap(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        assertThat(new ArrayLike.Slice<>(ints, 3, 2).size()).isEqualTo(0);
        assertThat(new ArrayLike.Slice<>(ints, 3, 3).size()).isEqualTo(0);
        assertThat(new ArrayLike.Slice<>(ints, 3, 4).size()).isEqualTo(1);
        assertThat(new ArrayLike.Slice<>(ints, 3, 100).size()).isEqualTo(7);
        assertThat(new ArrayLike.Slice<>(ints, 90, 100).size()).isEqualTo(0);
        assertThatThrownBy(() -> new ArrayLike.Slice<>(ints, 3, 2).get(0)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThat(new ArrayLike.Slice<>(ints, 3, 4).get(0)).isEqualTo(3);
        final Integer[] dst = new Integer[5];
        assertThatThrownBy(() -> new ArrayLike.Slice<>(ints, 3, 4).copyTo(1, dst, 0, 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        new ArrayLike.Slice<>(ints, 3, 4).copyTo(1, dst, 0, 0);
        qt().forAll(integers().between(0, 4), integers().between(5, 9), integers().between(0, 9), integers().between(0, 9))
                .assuming((i, j, srcPos, length) -> i <= j && srcPos + length < j - i)
                .checkAssert(
                        (i, j, srcPos, length) -> {
                            assertCopyToWorks(new ArrayLike.Slice<>(ints, i, j), srcPos, length);
                        });
    }

    @Test
    public void testMapped() {
        final ArrayLike<Integer> mapped = wrap(0, 1, 2).map(i -> i + 100);
        final Integer[] dst = new Integer[5];
        assertThatThrownBy(() -> mapped.copyTo(2, dst, 0, 5)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    public void testWith() {
        final ArrayLike<Integer> with = wrap(0, 1, 2).with(1, 1000);
        assertThat(with.get(0)).isEqualTo(0);
        assertThat(with.get(1)).isEqualTo(1000);
    }

    private void assertCopyToWorks(ArrayLike<Integer> arr, Integer srcPos, Integer length) {
        final Integer[] copy1 = Arrays.copyOfRange(arr.copyOut(Integer.class), srcPos, srcPos + length);
        final Integer[] copy2 = new Integer[length];
        arr.copyTo(srcPos, copy2, 0, length);
        assertThat(copy2).containsExactly(copy1);
    }
}
