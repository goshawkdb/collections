package io.goshawkdb.collections.test.btree;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import io.goshawkdb.client.Connection;
import io.goshawkdb.client.GoshawkObjRef;
import io.goshawkdb.collections.btree.BTree;
import io.goshawkdb.collections.test.CreateTestBase;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.function.BiConsumer;

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
}
