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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.goshawkdb.client.Connection;
import io.goshawkdb.client.RefCap;
import io.goshawkdb.client.Transactor;
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

    private void assertSize(final Transactor txr, final LinearHash lh, final int expected) throws Exception {
        assertEquals(expected, lh.size(txr));
    }

    @Test
    public void createNewTest() throws Exception {
        try {
            final Connection c = createConnections(1)[0];
            final LinearHash lh = create(c);
            assertSize(c, lh, 0);
        } finally {
            shutdown();
        }
    }

    @Test
    public void putGetTestForEach() throws Exception {
        try {
            final Connection c = createConnections(1)[0];
            final LinearHash lh = create(c);

            int objCount = 1024;
            // create objs
            final Map<String, RefCap> m = c.transact(txn -> {
                final Map<String, RefCap> n = new HashMap<String, RefCap>();
                for (int idx = 0; idx < objCount; idx++) {
                    final String str = "" + idx;
                    final RefCap objRef = txn.create(ByteBuffer.wrap(str.getBytes()));
                    if (txn.restartNeeded()) {
                        return null;
                    }
                    n.put(str, objRef);
                }
                return n;
            }).getResultOrRethrow();
            c.transact(txn -> {
                m.forEach((key, value) -> {
                    lh.put(txn, key.getBytes(), value);
                    if (txn.restartNeeded()) {
                        return;
                    }
                });
                return null;
            }).getResultOrRethrow();
            assertSize(c, lh, objCount);

            m.forEach((key, value) -> {
                final RefCap objRefFound = lh.find(c, key.getBytes());
                if (objRefFound == null) {
                    fail("Failed to find entry for " + key);
                } else if (!objRefFound.sameReferent(value)) {
                    fail("Entry for " + key + " has value in " + objRefFound + " instead of " + value);
                }
            });
            assertSize(c, lh, objCount);

            c.transact(txn -> {
                final Set<String> covered = new HashSet<String>();
                lh.forEach(txn, (key, value) -> {
                    final String str = new String(key);
                    if (covered.contains(str)) {
                        fail("forEach yielded key twice! " + str);
                    }
                    covered.add(str);
                    final RefCap ref = m.get(str);
                    if (ref == null) {
                        fail("forEach yielded unknown key: " + str);
                    } else if (!ref.sameReferent(value)) {
                        fail("forEach yielded unexpected value for key " + str + " (expected " + ref + "; actual " + value + ")");
                    }
                });
                if (covered.size() != m.size()) {
                    fail("forEach yielded incorrect number of entries: " + covered.size() + " vs " + m.size());
                }
                return null;
            }).getResultOrRethrow();
        } finally {
            shutdown();
        }
    }
}
