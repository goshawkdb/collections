package io.goshawkdb.collections.test;

import static org.junit.Assert.fail;

import io.goshawkdb.client.Connection;
import io.goshawkdb.client.GoshawkObjRef;
import io.goshawkdb.client.TransactionAbortedException;
import io.goshawkdb.client.TransactionResult;
import io.goshawkdb.test.TestBase;
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
import java.util.function.BiConsumer;
import org.junit.Test;

public abstract class CreateTestBase<T> extends TestBase {
    protected CreateTestBase()
            throws NoSuchProviderException, NoSuchAlgorithmException, CertificateException,
                    KeyStoreException, IOException, InvalidKeySpecException, InvalidKeyException {
        super();
    }

    protected abstract T create(final Connection c) throws Exception;

    protected abstract void assertSize(T collection, final int expected) throws Exception;

    protected abstract void put(T collection, byte[] bytes, GoshawkObjRef value) throws Exception;

    protected abstract GoshawkObjRef find(T collection, byte[] bytes) throws Exception;

    protected abstract void forEach(T collection, BiConsumer<byte[], GoshawkObjRef> action)
            throws Exception;

    @Test
    public void createNewTest() throws Exception {
        try {
            final Connection c = createConnections(1)[0];
            final T collection = create(c);
            assertSize(collection, 0);
        } finally {
            shutdown();
        }
    }

    @Test
    public void putGetTestForEach() throws Exception {
        try {
            final Connection c = createConnections(1)[0];
            final T collection = create(c);

            int objCount = 1024;
            // create objs
            TransactionResult<Map<String, GoshawkObjRef>> r0 =
                    c.runTransaction(
                            txn -> {
                                final Map<String, GoshawkObjRef> m = new HashMap<>();
                                for (int idx = 0; idx < objCount; idx++) {
                                    final String str = "" + idx;
                                    final GoshawkObjRef objRef =
                                            txn.createObject(ByteBuffer.wrap(str.getBytes()));
                                    m.put(str, objRef);
                                }
                                return m;
                            });
            if (!r0.isSuccessful()) {
                throw r0.cause;
            }
            final Map<String, GoshawkObjRef> m = r0.result;
            final TransactionResult<Object> r1 =
                    c.runTransaction(
                            txn -> {
                                m.forEach(
                                        (key, value) -> {
                                            value = txn.getObject(value);
                                            try {
                                                put(collection, key.getBytes(), value);
                                            } catch (Exception e) {
                                                throw new TransactionAbortedException(e);
                                            }
                                        });
                                return null;
                            });
            if (!r1.isSuccessful()) {
                throw r1.cause;
            }
            assertSize(collection, objCount);

            m.forEach(
                    (key, value) -> {
                        try {
                            final GoshawkObjRef objRefFound = find(collection, key.getBytes());
                            if (objRefFound == null) {
                                fail("Failed to find entry for " + key);
                            } else if (!objRefFound.referencesSameAs(value)) {
                                fail(
                                        "Entry for "
                                                + key
                                                + " has value in "
                                                + objRefFound
                                                + " instead of "
                                                + value);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
            assertSize(collection, objCount);

            final TransactionResult<Object> r2 =
                    c.runTransaction(
                            txn -> {
                                final Set<String> covered = new HashSet<>();
                                try {
                                    forEach(
                                            collection,
                                            (key, value) -> {
                                                final String str = new String(key);
                                                if (covered.contains(str)) {
                                                    fail("forEach yielded key twice! " + str);
                                                }
                                                covered.add(str);
                                                final GoshawkObjRef ref = m.get(str);
                                                if (ref == null) {
                                                    fail("forEach yielded unknown key: " + str);
                                                } else if (!ref.referencesSameAs(value)) {
                                                    fail(
                                                            "forEach yielded unexpected value for key "
                                                                    + str
                                                                    + " (expected "
                                                                    + ref
                                                                    + "; actual "
                                                                    + value
                                                                    + ")");
                                                }
                                            });
                                } catch (Exception e) {
                                    throw new TransactionAbortedException(e);
                                }
                                if (covered.size() != m.size()) {
                                    fail(
                                            "forEach yielded incorrect number of entries: "
                                                    + covered.size()
                                                    + " vs "
                                                    + m.size());
                                }
                                return null;
                            });
            if (!r2.isSuccessful()) {
                throw r2.cause;
            }
        } finally {
            shutdown();
        }
    }
}
