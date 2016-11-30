package io.goshawkdb.collections.test.btree;

import io.goshawkdb.client.Connection;
import io.goshawkdb.client.GoshawkObjRef;
import io.goshawkdb.client.TransactionResult;
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
    protected TransactionResult<BTree> create(Connection c) {
        return BTree.createEmpty(c);
    }

    @Override
    protected TransactionResult<Void> put(BTree t, byte[] key, GoshawkObjRef value) {
        return t.put(key, value);
    }

    @Override
    protected TransactionResult<GoshawkObjRef> find(BTree t, byte[] key) {
        return t.find(key);
    }

    @Override
    protected TransactionResult<Void> remove(BTree t, byte[] key) {
        return t.remove(key);
    }
}
