package io.goshawkdb.collections.test.linearhash;

import io.goshawkdb.client.Connection;
import io.goshawkdb.client.GoshawkObjRef;
import io.goshawkdb.client.TransactionResult;
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
    protected TransactionResult<LinearHash> create(Connection c) {
        return LinearHash.createEmpty(c);
    }

    @Override
    protected TransactionResult<Integer> size(LinearHash lh) {
        return lh.size();
    }

    @Override
    protected TransactionResult<Object> put(LinearHash lh, byte[] bytes, GoshawkObjRef value) {
        return lh.put(bytes, value);
    }

    @Override
    protected TransactionResult<GoshawkObjRef> find(LinearHash lh, byte[] bytes) {
        return lh.find(bytes);
    }

    @Override
    protected TransactionResult<Object> forEach(LinearHash lh, BiConsumer<byte[], GoshawkObjRef> action) {
        return lh.forEach(action);
    }
}
