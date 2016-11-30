package io.goshawkdb.collections.test.btree;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import io.goshawkdb.client.Connection;
import io.goshawkdb.client.GoshawkObjRef;
import io.goshawkdb.collections.btree.BTree;
import io.goshawkdb.collections.btree.Cursor;
import io.goshawkdb.collections.test.CreateTestBase;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import org.junit.Test;

public class CreateTest extends CreateTestBase<BTree> {
    public CreateTest()
            throws NoSuchProviderException, NoSuchAlgorithmException, CertificateException,
                    KeyStoreException, IOException, InvalidKeySpecException, InvalidKeyException {}

    @Override
    protected BTree create(Connection c) throws Exception {
        return BTree.createEmpty(c);
    }

    @Override
    protected void assertSize(BTree t, int expected) throws Exception {
        assertThat(t.size(), equalTo(expected));
    }

    @Override
    protected void put(BTree t, byte[] bytes, GoshawkObjRef value) throws Exception {
        t.put(bytes, value);
    }

    @Override
    protected GoshawkObjRef find(BTree t, byte[] key) throws Exception {
        return t.find(key);
    }

    @Override
    protected void forEach(BTree t, BiConsumer<byte[], GoshawkObjRef> action) throws Exception {
        t.forEach(action);
    }

    @Test
    public void testCursors() throws Exception {
        try {
            final Connection c = createConnections(1)[0];
            final BTree t = create(c);
            for (int i = 0; i < 10; i++) {
                t.put(Integer.toString(i).getBytes(), t.getRoot());
            }
            final Cursor<byte[], GoshawkObjRef> cursor = t.cursor();
            final ArrayList<Object> expected = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                expected.add(Integer.toString(i).getBytes());
            }
            assertThat(toList(cursor))
                    .filteredOn(e -> e instanceof byte[])
                    .containsExactlyElementsOf(expected);
        } finally {
            shutdown();
        }
    }

    private List<Object> toList(Cursor<?, ?> cursor) {
        final ArrayList<Object> r = new ArrayList<>();
        while (cursor.inTree()) {
            r.add(cursor.getKey());
            r.add(cursor.getValue());
            cursor.moveRight();
        }
        return r;
    }
}
