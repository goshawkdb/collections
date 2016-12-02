package io.goshawkdb.collections.linearhash;

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.SecureRandom;

import io.goshawkdb.client.TransactionAbortedException;

final class Root {
    static final int BucketCapacity = 64;
    static final double UtilizationFactor = 0.75;
    static final int SipHashKeyLength = 16;

    final int fieldCount = 6;
    int size;
    int bucketCount;
    BigInteger splitIndex;
    BigInteger maskHigh;
    BigInteger maskLow;
    byte[] hashkey;

    Root() {
        size = 0;
        bucketCount = 2;
        splitIndex = BigInteger.ZERO;
        maskHigh = BigInteger.valueOf(3);
        maskLow = BigInteger.valueOf(1);
        final SecureRandom rng = new SecureRandom();
        hashkey = new byte[SipHashKeyLength];
        rng.nextBytes(hashkey);
    }

    Root(final ByteBuffer data) {
        try (final MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(data)) {
            while (unpacker.hasNext()) {
                MessageFormat f = unpacker.getNextFormat();
                if (!(f == MessageFormat.FIXMAP || f == MessageFormat.MAP16 || f == MessageFormat.MAP32)) {
                    throw new IllegalArgumentException("data does not contain a LinearHash root");
                }
                int pairs = unpacker.unpackMapHeader();
                if (pairs != fieldCount) {
                    throw new IllegalArgumentException("Expected " + fieldCount + " pairs in root map. Found " + pairs);
                }
                for (; pairs > 0; pairs--) {
                    final String key = unpacker.unpackString();
                    switch (key) {
                        case "Size":
                            size = unpacker.unpackInt();
                            break;
                        case "BucketCount":
                            bucketCount = unpacker.unpackInt();
                            break;
                        case "SplitIndex":
                            splitIndex = unpacker.unpackBigInteger();
                            break;
                        case "MaskHigh":
                            maskHigh = unpacker.unpackBigInteger();
                            break;
                        case "MaskLow":
                            maskLow = unpacker.unpackBigInteger();
                            break;
                        case "HashKey":
                            hashkey = unpacker.readPayload(unpacker.unpackBinaryHeader());
                            break;
                        default:
                            throw new IllegalArgumentException("Unexpected key in LinearHash root: " + key);
                    }
                }
            }
        } catch (Exception e) {
            throw new TransactionAbortedException(e);
        }
    }

    ByteBuffer pack() {
        try (final MessageBufferPacker packer = MessagePack.newDefaultBufferPacker()) {
            packer.packMapHeader(fieldCount);
            packer.packString("Size");
            packer.packInt(size);
            packer.packString("BucketCount");
            packer.packInt(bucketCount);
            packer.packString("SplitIndex");
            packer.packBigInteger(splitIndex);
            packer.packString("MaskHigh");
            packer.packBigInteger(maskHigh);
            packer.packString("MaskLow");
            packer.packBigInteger(maskLow);
            packer.packString("HashKey");
            packer.packBinaryHeader(hashkey.length);
            packer.writePayload(hashkey);
            return ByteBuffer.wrap(packer.toByteArray());
        } catch (IOException e) {
            throw new TransactionAbortedException(e);
        }
    }

    int bucketIndex(final BigInteger key) {
        final BigInteger hashLow = key.and(maskLow);
        // test whether hashLow >= splitIndex
        if (hashLow.compareTo(splitIndex) >= 0) {
            return hashLow.intValueExact();
        } else {
            return key.and(maskHigh).intValueExact();
        }
    }

    boolean needsSplit() {
        return ((double) size / (double) (BucketCapacity * bucketCount)) > UtilizationFactor;
    }
}
