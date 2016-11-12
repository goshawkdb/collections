package io.goshawkdb.collections.test.btree;

import io.goshawkdb.client.Connection;
import io.goshawkdb.client.GoshawkObjRef;
import io.goshawkdb.client.Transaction;
import io.goshawkdb.client.TransactionAbortedException;
import io.goshawkdb.client.TransactionResult;
import io.goshawkdb.collections.btree.BTree;
import io.goshawkdb.test.TestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class BTreeTest extends TestBase {

    private Connection c;

    public BTreeTest() throws NoSuchProviderException, NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException, InvalidKeySpecException, InvalidKeyException {
        super();
    }

    @Before
    public void before() throws Exception {
        c = createConnections(1)[0];
        runTransaction(c, txn -> {
            final BTree t;
            try {
                t = BTree.createEmpty(txn);
            } catch (Exception e) {
                throw new TransactionAbortedException(e);
            }
            getRoot(txn).set(ByteBuffer.allocate(0), t.getRoot());
            return null;
        });
    }

    @After
    public void after() throws Exception {
        shutdown();
    }

    @Test
    public void createNewTest() throws Exception {
        assertThat(theTreeSize(), equalTo(0));
    }

    private Integer theTreeSize() {
        return runTransaction(c, txn -> {
            try {
                return theTree(txn).count();
            } catch (Exception e) {
                throw new TransactionAbortedException(e);
            }
        });
    }

    private BTree theTree(final Transaction txn) {
        return new BTree(txn, getRoot(txn).getReferences()[0]);
    }

    @Test
    public void putGetTestForEach() throws Exception {
        int objCount = 1024;
        // create objs
        TransactionResult<Map<String, GoshawkObjRef>> r0 = c.runTransaction(txn -> {
            final Map<String, GoshawkObjRef> m = new HashMap<String, GoshawkObjRef>();
            for (int idx = 0; idx < objCount; idx++) {
                final String str = "" + idx;
                final GoshawkObjRef objRef = txn.createObject(ByteBuffer.wrap(str.getBytes()));
                m.put(str, objRef);
            }
            return m;
        });
        if (!r0.isSuccessful()) {
            throw r0.cause;
        }
        final Map<String, GoshawkObjRef> m = r0.result;
        final TransactionResult<Object> r1 = c.runTransaction(txn -> {
            final BTree t = theTree(txn);
            m.forEach((key, value) -> {
                value = txn.getObject(value);
                try {
                    t.put(key.getBytes(), value);
                } catch (Exception e) {
                    throw new TransactionAbortedException(e);
                }
            });
            return null;
        });
        if (!r1.isSuccessful()) {
            throw r1.cause;
        }
        assertThat(theTreeSize(), equalTo(objCount));

        runTransaction(c, txn -> {
            final BTree t = theTree(txn);
            m.forEach((key, value) -> {
                try {
                    final GoshawkObjRef objRefFound = t.find(key.getBytes());
                    if (objRefFound == null) {
                        fail("Failed to find entry for " + key);
                    } else if (!objRefFound.referencesSameAs(value)) {
                        fail("Entry for " + key + " has value in " + objRefFound + " instead of " + value);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            return null;
        });
        assertThat(theTreeSize(), equalTo(objCount));

        final TransactionResult<Object> r2 = c.runTransaction(txn -> {
            final BTree t = theTree(txn);
            final Set<String> covered = new HashSet<String>();
            try {
                t.forEach((key, value) -> {
                    final String str = new String(key);
                    if (covered.contains(str)) {
                        fail("forEach yielded key twice! " + str);
                    }
                    covered.add(str);
                    final GoshawkObjRef ref = m.get(str);
                    if (ref == null) {
                        fail("forEach yielded unknown key: " + str);
                    } else if (!ref.referencesSameAs(value)) {
                        fail("forEach yielded unexpected value for key " + str + " (expected " + ref + "; actual " + value + ")");
                    }
                });
            } catch (Exception e) {
                throw new TransactionAbortedException(e);
            }
            if (covered.size() != m.size()) {
                fail("forEach yielded incorrect number of entries: " + covered.size() + " vs " + m.size());
            }
            return null;
        });
        if (!r2.isSuccessful()) {
            throw r2.cause;
        }
    }
}