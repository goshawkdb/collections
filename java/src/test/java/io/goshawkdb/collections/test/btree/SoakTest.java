package io.goshawkdb.collections.test.btree;

import io.goshawkdb.client.Connection;
import io.goshawkdb.client.GoshawkObjRef;
import io.goshawkdb.client.Transaction;
import io.goshawkdb.collections.btree.BTree;
import io.goshawkdb.collections.test.SoakTestBase;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;

public class SoakTest extends SoakTestBase<SoakTest.ConnAndRef> {
    public SoakTest() throws NoSuchProviderException, NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException, InvalidKeySpecException, InvalidKeyException {
    }

    static class ConnAndRef {
        private final Connection conn;
        private final GoshawkObjRef ref;

        ConnAndRef(Connection conn, GoshawkObjRef ref) {
            this.conn = conn;
            this.ref = ref;
        }

        BTree getTree(Transaction txn) {
            return new BTree(txn, txn.getObject(ref));
        }
    }

    @Override
    protected ConnAndRef create(Connection c) throws Exception {
        return runTransaction(c, txn -> new ConnAndRef(c, BTree.createEmpty(txn).getRoot()));
    }

    @Override
    protected void put(ConnAndRef cr, byte[] bytes, GoshawkObjRef value) throws Exception {
        runTransaction(cr.conn, txn -> {
            cr.getTree(txn).put(bytes, value);
            return null;
        });
    }

    @Override
    protected GoshawkObjRef find(ConnAndRef cr, byte[] bytes) throws Exception {
        return runTransaction(cr.conn, txn -> cr.getTree(txn).find(bytes));
    }

    @Override
    protected void remove(ConnAndRef collection, byte[] bytes) throws Exception {

    }
}
