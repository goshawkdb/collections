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

import io.goshawkdb.client.Connection;
import io.goshawkdb.client.RefCap;
import io.goshawkdb.client.ValueRefs;
import io.goshawkdb.collections.linearhash.LinearHash;
import io.goshawkdb.test.TestBase;

public class SoakTest extends TestBase {

    public SoakTest() throws NoSuchProviderException, NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException, InvalidKeySpecException, InvalidKeyException {
        super();
    }

    @Test
    public void soak() throws Exception {
        try {
            final Connection c = createConnections(1)[0];
            LinearHash lh = LinearHash.createEmpty(c);

            final long seed = System.nanoTime();
            System.out.println("Seed: " + seed);
            final Random rng = new Random(seed);
            // we use contents to mirror the state of the LHash
            final Map<String, String> contents = new HashMap<String, String>();

            for (int i = 16384; i > 0; i--) {
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
                    lh = LinearHash.createEmpty(c);
                    contents.clear();

                } else if (op < -1) { // add new key
                    final String key = String.valueOf(contentsSize);
                    final String value = "Hello" + i + "-" + key;
                    final LinearHash finalLh = lh;
                    c.transact(txn -> {
                        RefCap valueObj = txn.create(ByteBuffer.wrap(value.getBytes()));
                        if (txn.restartNeeded()) {
                            return null;
                        }
                        finalLh.put(txn, key.getBytes(), valueObj);
                        return null;
                    }).getResultOrRethrow();
                    contents.put(key, value);

                } else {
                    switch (opClass) {
                        case 0: { // find key
                            final String key = String.valueOf(opArg);
                            final String value = contents.get(key);
                            final boolean inContents = !"".equals(value);
                            final LinearHash finalLh = lh;
                            final String result = c.transact(txn -> {
                                final RefCap objRef = finalLh.find(txn, key.getBytes());
                                if (txn.restartNeeded()) {
                                    return null;
                                } else if (objRef == null) {
                                    return null;
                                } else {
                                    final ValueRefs objVR = txn.read(objRef);
                                    if (txn.restartNeeded()) {
                                        return null;
                                    }
                                    final ByteBuffer bb = objVR.value;
                                    return byteBufferToString(bb, bb.limit());
                                }
                            }).getResultOrRethrow();
                            if (inContents && (result == null || !result.equals(value))) {
                                throw new IllegalStateException(key + ": Failed to retrieve string value: " + result);
                            } else if (!inContents && result != null) {
                                throw new IllegalStateException(key + ": Got result even after remove: " + result);
                            }
                            break;
                        }

                        case 1: { // remove key
                            final String key = String.valueOf(opArg);
                            final String value = contents.get(key);
                            final boolean inContents = !"".equals(value);
                            lh.remove(c, key.getBytes());
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
                            final LinearHash finalLh = lh;
                            c.transact(txn -> {
                                RefCap valueObj = txn.create(ByteBuffer.wrap(finalValue.getBytes()));
                                if (txn.restartNeeded()) {
                                    return null;
                                }
                                finalLh.put(txn, key.getBytes(), valueObj);
                                return null;
                            }).getResultOrRethrow();
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
