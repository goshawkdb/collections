package io.goshawkdb.collections.test.linearhash;

import io.goshawkdb.client.Connection;
import io.goshawkdb.client.GoshawkObjRef;
import io.goshawkdb.collections.linearhash.LinearHash;
import io.goshawkdb.collections.test.SoakTestBase;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;

public class SoakTest extends SoakTestBase<LinearHash> {
    public SoakTest() throws NoSuchProviderException, NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException, InvalidKeySpecException, InvalidKeyException {
    }

    @Override
    protected LinearHash create(Connection c) throws Exception {
        return LinearHash.createEmpty(c);
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
    protected void remove(LinearHash lh, byte[] bytes) throws Exception {
        lh.remove(bytes);
    }
}
