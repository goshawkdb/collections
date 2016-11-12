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
import java.util.Random;
import java.util.function.BiConsumer;

import io.goshawkdb.client.Connection;
import io.goshawkdb.client.GoshawkObjRef;
import io.goshawkdb.client.TransactionAbortedException;
import io.goshawkdb.client.TransactionResult;
import io.goshawkdb.collections.linearhash.LinearHash;
import io.goshawkdb.test.TestBase;

public abstract class SoakTestBase<T> extends TestBase {
    protected SoakTestBase() throws NoSuchProviderException, NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException, InvalidKeySpecException, InvalidKeyException {
        super();
    }

    protected abstract T create(final Connection c) throws Exception;

    protected abstract void put(T collection, byte[] bytes, GoshawkObjRef value) throws Exception;

    protected abstract GoshawkObjRef find(T collection, byte[] bytes) throws Exception;

    protected abstract void remove(T collection, byte[] bytes) throws Exception;

    @Test
    public void soak() throws Exception {
        try {
            final Connection c = createConnections(1)[0];
            T collection = create(c);

            final long seed = System.nanoTime();
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
                    collection = create(c);
                    contents.clear();

                } else if (op < -1) { // add new key
                    final String key = String.valueOf(contentsSize);
                    final String value = "Hello" + i + "-" + key;
                    final T finalCollection = collection;
                    TransactionResult<Object> result = c.runTransaction(txn -> {
                        GoshawkObjRef valueObj = txn.createObject(ByteBuffer.wrap(value.getBytes()));
                        try {
                            put(finalCollection, key.getBytes(), valueObj);
                        } catch (Exception e) {
                            throw new TransactionAbortedException(e);
                        }
                        return null;
                    });
                    if (!result.isSuccessful()) {
                        throw result.cause;
                    }
                    contents.put(key, value);

                } else {
                    switch (opClass) {
                        case 0: { // find key
                            final String key = String.valueOf(opArg);
                            final String value = contents.get(key);
                            final boolean inContents = !"".equals(value);
                            final T finalCollection = collection;
                            final TransactionResult<String> result = c.runTransaction(txn -> {
                                try {
                                    final GoshawkObjRef valueObj = find(finalCollection, key.getBytes());
                                    if (valueObj == null) {
                                        return null;
                                    } else {
                                        final ByteBuffer bb = valueObj.getValue();
                                        return byteBufferToString(bb, bb.limit());
                                    }
                                } catch (Exception e) {
                                    throw new TransactionAbortedException(e);
                                }
                            });
                            if (!result.isSuccessful()) {
                                throw result.cause;
                            }
                            if (inContents && (result.result == null || !result.result.equals(value))) {
                                throw new IllegalStateException(key + ": Failed to retrieve string value: " + result.result);
                            } else if (!inContents && result.result != null) {
                                throw new IllegalStateException(key + ": Got result even after remove: " + result.result);
                            }
                            break;
                        }

                        case 1: { // remove key
                            final String key = String.valueOf(opArg);
                            final String value = contents.get(key);
                            final boolean inContents = !"".equals(value);
                            remove(collection, key.getBytes());
                            if (inContents) {
                                contents.put(key, "");
                            }
                            break;
                        }

                        case 2: { // re-put existing key
                            final String key = String.valueOf(opArg);
                            String value = contents.get(key);
                            final boolean inContents = !"".equals(value);
                            if (!inContents) {
                                value = "Hello" + i + "-" + key;
                                contents.put(key, value);
                            }
                            final String finalValue = value;
                            final T finalCollection = collection;
                            final TransactionResult<Object> result = c.runTransaction(txn -> {
                                GoshawkObjRef valueObj = txn.createObject(ByteBuffer.wrap(finalValue.getBytes()));
                                try {
                                    put(finalCollection, key.getBytes(), valueObj);
                                } catch (Exception e) {
                                    throw new TransactionAbortedException(e);
                                }
                                return null;
                            });
                            if (!result.isSuccessful()) {
                                throw result.cause;
                            }
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
