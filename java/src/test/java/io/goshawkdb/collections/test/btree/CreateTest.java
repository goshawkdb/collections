package io.goshawkdb.collections.test.btree;

import static org.assertj.core.api.Assertions.assertThat;

import io.goshawkdb.client.Connection;
import io.goshawkdb.client.GoshawkObjRef;
import io.goshawkdb.client.TransactionResult;
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
            throws NoSuchProviderException, NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException,
                    InvalidKeySpecException, InvalidKeyException {}

    @Override
    protected TransactionResult<BTree> create(Connection c) {
        return BTree.createEmpty(c);
    }

    @Override
    protected TransactionResult<Integer> size(BTree t) {
        return t.size();
    }

    @Override
    protected TransactionResult<Void> put(BTree t, byte[] bytes, GoshawkObjRef value) {
        return t.put(bytes, value);
    }

    @Override
    protected TransactionResult<GoshawkObjRef> find(BTree t, byte[] key) {
        return t.find(key);
    }

    @Override
    protected TransactionResult<Void> forEach(BTree t, BiConsumer<byte[], GoshawkObjRef> action) {
        return t.forEach(action);
    }

    @Test
    public void testCursors() throws Exception {
        try {
            final Connection c = createConnections(1)[0];
            final BTree t = create(c).getResultOrRethrow();
            for (int i = 0; i < 10; i++) {
                t.put(Integer.toString(i).getBytes(), t.getRoot());
            }
            final Cursor<byte[], GoshawkObjRef> cursor = t.cursor();
            final ArrayList<Object> expected = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                expected.add(Integer.toString(i).getBytes());
            }
            assertThat(toList(cursor)).filteredOn(e -> e instanceof byte[]).containsExactlyElementsOf(expected);
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
