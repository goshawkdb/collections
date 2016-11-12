package io.goshawkdb.collections.test.btree;

import io.goshawkdb.collections.btree.Lexicographic;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class LexicographicTest {
    @Test
    public void testCompare() {
        assertThat(Lexicographic.INSTANCE.compare(new byte[]{}, new byte[]{}), is(0));
        assertThat(Lexicographic.INSTANCE.compare(new byte[]{}, new byte[]{1}), is(-1));
        assertThat(Lexicographic.INSTANCE.compare(new byte[]{0}, new byte[]{1}), is(-1));
        assertThat(Lexicographic.INSTANCE.compare(new byte[]{0}, new byte[]{0, 0}), is(-1));
        assertThat(Lexicographic.INSTANCE.compare(new byte[]{0}, new byte[]{}), is(1));
        assertThat(Lexicographic.INSTANCE.compare(new byte[]{1}, new byte[]{0}), is(1));
    }
}