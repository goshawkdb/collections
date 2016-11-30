package io.goshawkdb.collections.test.btree;

import io.goshawkdb.collections.btree.Lexicographic;
import io.goshawkdb.collections.btree.MemBTree;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.junit.Test;

// XXX: copy-pasta from SoakTestBase
public class MemSoakTest {
    private MemBTree<byte[]> create() throws Exception {
        return new MemBTree<>(16, Lexicographic.INSTANCE);
    }

    @Test
    public void soak() throws Exception {
        MemBTree<byte[]> collection = create();

        final long seed = System.nanoTime();
        final Random rng = new Random(seed);
        // we use contents to mirror the state of the LHash
        final Map<String, String> contents = new HashMap<>();

        for (int i = 1000000; i > 0; i--) {
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
                collection = create();
                contents.clear();

            } else if (op < -1) { // add new key
                final String key = String.valueOf(contentsSize);
                final String value = "Hello" + i + "-" + key;
                collection.put(key.getBytes(), value.getBytes());
                contents.put(key, value);
            } else {
                switch (opClass) {
                    case 0:
                        { // find key
                            final String key = String.valueOf(opArg);
                            final String value = contents.get(key);
                            final boolean inContents = !"".equals(value);
                            final Object valueObj = collection.find(key.getBytes());
                            if (inContents && (valueObj == null || !value.equals(new String((byte[]) valueObj)))) {
                                throw new IllegalStateException(
                                        String.format(
                                                "%s: Failed to retrieve string value: got %s, expected %s",
                                                key, new String((byte[]) valueObj), value));
                            } else if (!inContents && valueObj != null) {
                                throw new IllegalStateException(key + ": Got result even after remove: " + valueObj);
                            }
                            break;
                        }

                    case 1:
                        { // remove key
                            final String key = String.valueOf(opArg);
                            final String value = contents.get(key);
                            final boolean inContents = !"".equals(value);
                            collection.remove(key.getBytes());
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
                            collection.put(key.getBytes(), value.getBytes());
                            break;
                        }

                    default:
                        throw new IllegalArgumentException("Impossible opClass: " + opClass);
                }
            }
        }
    }
}
