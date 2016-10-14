package io.goshawkdb.collections.test;

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
import java.util.Map;

import io.goshawkdb.client.Connection;
import io.goshawkdb.client.GoshawkObjRef;
import io.goshawkdb.client.TransactionAbortedException;
import io.goshawkdb.client.TransactionResult;
import io.goshawkdb.collections.linearhash.LinearHash;
import io.goshawkdb.test.TestBase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CreateTest extends TestBase {

    public CreateTest() throws NoSuchProviderException, NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException, InvalidKeySpecException, InvalidKeyException {
        super();
    }

    private LinearHash create(final Connection c) throws Exception {
        return LinearHash.createEmpty(c);
    }

    private void assertSize(final LinearHash lh, final int expected) throws Exception {
        assertEquals(expected, lh.size());
    }

    @Test
    public void createNewTest() throws Exception {
        try {
            final Connection c = createConnections(1)[0];
            final LinearHash lh = create(c);
            assertSize(lh, 0);
        } finally {
            shutdown();
        }
    }

    @Test
    public void putGetTest() throws Exception {
        try {
            final Connection c = createConnections(1)[0];
            final LinearHash lh = create(c);

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
                m.forEach((key, value) -> {
                    value = txn.getObject(value);
                    try {
                        lh.put(key.getBytes(), value);
                    } catch (Exception e) {
                        throw new TransactionAbortedException(e);
                    }
                });
                return null;
            });
            if (!r1.isSuccessful()) {
                throw r1.cause;
            }
            assertSize(lh, objCount);

            m.forEach((key, value) -> {
                try {
                    final GoshawkObjRef objRefFound = lh.find(key.getBytes());
                    if (objRefFound == null) {
                        fail("Failed to find entry for " + key);
                    } else if (!objRefFound.referencesSameAs(value)) {
                        fail("Entry for " + key + " has value in " + objRefFound + " instead of " + value);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            assertSize(lh, objCount);
        } finally {
            shutdown();
        }
    }
}
