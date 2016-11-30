package io.goshawkdb.collections.test;

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
import java.util.Map;
import java.util.Random;
import org.junit.Test;

public abstract class SoakTestBase<T> extends TestBase {
    protected SoakTestBase()
            throws NoSuchProviderException, NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException,
                    InvalidKeySpecException, InvalidKeyException {
        super();
    }

    protected abstract TransactionResult<T> create(final Connection c);

    protected abstract TransactionResult<Object> put(T collection, byte[] bytes, GoshawkObjRef value);

    protected abstract TransactionResult<GoshawkObjRef> find(T collection, byte[] bytes);

    protected abstract TransactionResult<Object> remove(T collection, byte[] bytes);

    @Test
    public void soak() throws Exception {
        try {
            final Connection c = createConnections(1)[0];
            T collection = create(c).getResultOrRethrow();

            final long seed = System.nanoTime();
            System.out.println("Seed: " + seed);
            final Random rng = new Random(seed);
            // we use contents to mirror the state of the LHash
            final Map<String, String> contents = new HashMap<>();

            for (int i = 4096; i > 0; i--) {
                final int contentsSize = contents.size();
                // we bias creation of new keys by 999 with 1 more for reset
                final int op = rng.nextInt((3 * contentsSize) + 1000) - 1000;
                int opClass = 0;
                int opArg = 0;
                if (contentsSize > 0) {
                    opClass = op / contentsSize;
                    opArg = op % contentsSize;
                }

                if (op == -1) { // reset
                    collection = create(c).getResultOrRethrow();
                    contents.clear();

                } else if (op < -1) { // add new key
                    final String key = String.valueOf(contentsSize);
                    final String value = "Hello" + i + "-" + key;
                    final T finalCollection = collection;
                    c.runTransaction(
                                    txn -> {
                                        GoshawkObjRef valueObj = txn.createObject(ByteBuffer.wrap(value.getBytes()));
                                        try {
                                            put(finalCollection, key.getBytes(), valueObj);
                                        } catch (Exception e) {
                                            throw new TransactionAbortedException(e);
                                        }
                                        return null;
                                    })
                            .getResultOrRethrow();
                    contents.put(key, value);

                } else {
                    switch (opClass) {
                        case 0:
                            { // find key
                                final String key = String.valueOf(opArg);
                                final String value = contents.get(key);
                                final boolean inContents = !"".equals(value);
                                final T finalCollection = collection;
                                final String result =
                                        c.runTransaction(
                                                        txn -> {
                                                            final GoshawkObjRef valueObj =
                                                                    find(finalCollection, key.getBytes()).getResultOrAbort();
                                                            if (valueObj == null) {
                                                                return null;
                                                            } else {
                                                                final ByteBuffer bb = valueObj.getValue();
                                                                return byteBufferToString(bb, bb.limit());
                                                            }
                                                        })
                                                .getResultOrRethrow();
                                if (inContents && (result == null || !result.equals(value))) {
                                    throw new IllegalStateException(key + ": Failed to retrieve string value: " + result);
                                } else if (!inContents && result != null) {
                                    throw new IllegalStateException(key + ": Got result even after remove: " + result);
                                }
                                break;
                            }

                        case 1:
                            { // remove key
                                final String key = String.valueOf(opArg);
                                final String value = contents.get(key);
                                final boolean inContents = !"".equals(value);
                                remove(collection, key.getBytes());
                                if (inContents) {
                                    contents.put(key, "");
                                }
                                break;
                            }

                        case 2:
                            { // re-put existing key
                                final String key = String.valueOf(opArg);
                                String value = contents.get(key);
                                final boolean inContents = !"".equals(value);
                                if (!inContents) {
                                    value = "Hello" + i + "-" + key;
                                    contents.put(key, value);
                                }
                                final String finalValue = value;
                                final T finalCollection = collection;
                                c.runTransaction(
                                                txn -> {
                                                    GoshawkObjRef valueObj =
                                                            txn.createObject(ByteBuffer.wrap(finalValue.getBytes()));
                                                    put(finalCollection, key.getBytes(), valueObj).getResultOrAbort();
                                                    return null;
                                                })
                                        .getResultOrRethrow();
                                break;
                            }

                        default:
                            throw new IllegalArgumentException("Impossible opClass: " + opClass);
                    }
                }
            }
        } finally {
            shutdown();
        }
    }
}
