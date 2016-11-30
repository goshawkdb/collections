package io.goshawkdb.collections.test.linearhash;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import io.goshawkdb.client.Connection;
import io.goshawkdb.client.GoshawkObjRef;
import io.goshawkdb.collections.linearhash.LinearHash;
import io.goshawkdb.collections.test.CreateTestBase;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.function.BiConsumer;

public class CreateTest extends CreateTestBase<LinearHash> {
    public CreateTest()
            throws NoSuchProviderException, NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException,
                    InvalidKeySpecException, InvalidKeyException {}

    @Override
    protected LinearHash create(Connection c) throws Exception {
        return LinearHash.createEmpty(c);
    }

    @Override
    protected void assertSize(LinearHash lh, int expected) throws Exception {
        assertThat(lh.size(), equalTo(expected));
    }

    @Override
    protected void put(LinearHash lh, byte[] bytes, GoshawkObjRef value) throws Exception {
        lh.put(bytes, value);
    }

    @Override
    protected GoshawkObjRef find(LinearHash lh, byte[] bytes) throws Exception {
        return lh.find(bytes);
    }

    @Override
    protected void forEach(LinearHash lh, BiConsumer<byte[], GoshawkObjRef> action) throws Exception {
        lh.forEach(action);
    }
}
