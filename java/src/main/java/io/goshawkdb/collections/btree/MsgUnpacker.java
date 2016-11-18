package io.goshawkdb.collections.btree;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

// convenience wrapper for MessageUnpacker without all the checked exceptions
class MsgUnpacker {
    private final MessageUnpacker unpacker;

    MsgUnpacker(ByteBuffer buffer) {
        this.unpacker = MessagePack.newDefaultUnpacker(buffer);
    }

    int unpackArrayHeader() {
        try {
            return unpacker.unpackArrayHeader();
        } catch (IOException e) {
            throw new MsgPackException(e);
        }
    }

    byte[] readBinary() {
        try {
            return unpacker.readPayload(unpacker.unpackBinaryHeader());
        } catch (IOException e) {
            throw new MsgPackException(e);
        }
    }

    boolean hasNext() {
        try {
            return unpacker.hasNext();
        } catch (IOException e) {
            throw new MsgPackException(e);
        }
    }
}
