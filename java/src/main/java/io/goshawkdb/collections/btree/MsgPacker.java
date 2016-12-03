package io.goshawkdb.collections.btree;

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;

import java.io.IOException;
import java.nio.ByteBuffer;

// convenience wrapper for MessageBufferPacker without all the checked exceptions
class MsgPacker {
    private final MessageBufferPacker packer;

    MsgPacker() {
        this.packer = MessagePack.newDefaultBufferPacker();
    }

    void packArrayHeader(int n) {
        try {
            packer.packArrayHeader(n);
        } catch (IOException e) {
            throw new MsgPackException(e);
        }
    }

    void writeBinary(byte[] bytes) {
        try {
            packer.packBinaryHeader(bytes.length);
            packer.writePayload(bytes);
        } catch (IOException e) {
            throw new MsgPackException(e);
        }
    }

    ByteBuffer toByteBuffer() {
        return ByteBuffer.wrap(packer.toByteArray());
    }
}
