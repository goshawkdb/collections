package io.goshawkdb.collections.test.btree;

import io.goshawkdb.client.Connection;
import io.goshawkdb.client.GoshawkObjRef;
import io.goshawkdb.collections.btree.BTree;
import io.goshawkdb.collections.test.SoakTestBase;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;

public class SoakTest extends SoakTestBase<BTree> {
    public SoakTest()
            throws NoSuchProviderException, NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException,
                    InvalidKeySpecException, InvalidKeyException {}

    @Override
    protected BTree create(Connection c) throws Exception {
        return BTree.createEmpty(c);
    }

    @Override
    protected void put(BTree t, byte[] key, GoshawkObjRef value) throws Exception {
        t.put(key, value);
    }

    @Override
    protected GoshawkObjRef find(BTree t, byte[] key) throws Exception {
        return t.find(key);
    }

    @Override
    protected void remove(BTree t, byte[] key) throws Exception {
        t.remove(key);
    }
}
