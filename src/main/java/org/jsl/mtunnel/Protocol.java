package org.jsl.mtunnel;

import org.jsl.collider.RetainableByteBuffer;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;

public class Protocol {
    public static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;
    static final short HEADER_SIZE = ((Short.SIZE / Byte.SIZE) * 2);

    static class Message {
        static ByteBuffer create(short type, int dataSize) {
            final int messageSize = (HEADER_SIZE + dataSize);
            assert(messageSize <= Short.toUnsignedInt((short)-1));
            final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(messageSize);
            byteBuffer.order(BYTE_ORDER);
            byteBuffer.putShort((short)messageSize);
            byteBuffer.putShort(type);
            return byteBuffer;
        }

        static int getLength(ByteBuffer msg) {
            return msg.getShort(msg.position());
        }

        static short getMessageId(RetainableByteBuffer msg) {
            return msg.getShort(msg.position() + 2);
        }
    }

    static class Ping {
        static final short ID = 1;

        static ByteBuffer create() {
            final ByteBuffer msg = Message.create(ID, 0);
            return msg.rewind();
        }
    }

    static class Pong {
        static final short ID = 2;

        static ByteBuffer create() {
            final ByteBuffer msg = Message.create(ID, 0);
            return msg.rewind();
        }
    }

    static class JoinRequest {
        static final short ID = 5;

        static ByteBuffer create(ArrayList<InetSocketAddress> groups) throws Exception {
            if (groups.size() > Byte.toUnsignedInt((byte)-1)) {
                throw new Exception("maximum number of groups exceeded");
            }
            int dataSize = 1;
            for (InetSocketAddress addr: groups) {
                final byte [] rawAddress = addr.getAddress().getAddress();
                dataSize += 1; // raw address length
                dataSize += rawAddress.length;
                dataSize += 2; // port number
            }
            final ByteBuffer msg = Message.create(ID, dataSize);
            msg.put((byte)groups.size());
            for (InetSocketAddress addr: groups) {
                final byte [] rawAddress = addr.getAddress().getAddress();
                msg.put((byte) rawAddress.length);
                msg.put(rawAddress);
                msg.putShort((short)addr.getPort());
            }
            return msg.rewind();
        }

        static InetSocketAddress [] getGroups(RetainableByteBuffer msg) throws UnknownHostException {
            msg.position(msg.position() + HEADER_SIZE);
            int count = Byte.toUnsignedInt(msg.get());
            final InetSocketAddress [] ret = new InetSocketAddress[count];
            for (int idx=0; idx<count; idx++) {
                final int rawAddrLength = Byte.toUnsignedInt(msg.get());
                final byte [] rawAddr = new byte[rawAddrLength];
                msg.get(rawAddr);
                final InetAddress addr = InetAddress.getByAddress(rawAddr);
                final int portNumber = Short.toUnsignedInt(msg.getShort());
                ret[idx] = new InetSocketAddress(addr, portNumber);
            }
            return ret;
        }
    }

    static class JoinResponse {
        static final short ID = 6;

        static ByteBuffer create(String statusText) throws Exception {
            ByteBuffer statusTextBB = null;
            int dataSize = (Short.SIZE / Byte.SIZE);
            if (statusText != null) {
                final CharsetEncoder encoder = Charset.defaultCharset().newEncoder();
                statusTextBB = encoder.encode(CharBuffer.wrap(statusText));
                dataSize += statusTextBB.remaining();
            }
            ByteBuffer msg = Message.create(ID, dataSize);
            msg.putShort((short)dataSize);
            if (statusTextBB != null) {
                msg.put(statusTextBB);
            }
            return msg.rewind();
        }

        static String getStatusText(RetainableByteBuffer msg) throws CharacterCodingException {
            msg.position(msg.position() + HEADER_SIZE);
            final int statusTextLength = Short.toUnsignedInt(msg.getShort());
            if (statusTextLength == 0) {
                return null;
            } else {
                final CharsetDecoder decoder = Charset.defaultCharset().newDecoder();
                msg.limit(msg.position() + statusTextLength);
                return decoder.decode(msg.getNioByteBuffer()).toString();
            }
        }
    }

    static class MulticastPacket {
        static final short ID = 7;

        static ByteBuffer create(InetSocketAddress groupAddr, RetainableByteBuffer packet) {
            final byte [] rawAddr = groupAddr.getAddress().getAddress();
            final int dataSize = (Short.SIZE / Byte.SIZE) + rawAddr.length + (Short.SIZE / Byte.SIZE) + packet.remaining();
            final ByteBuffer msg = Message.create(ID, dataSize);
            msg.putShort((short)rawAddr.length);
            msg.put(rawAddr);
            msg.putShort((short)groupAddr.getPort());
            msg.put(packet.getNioByteBuffer());
            return msg.rewind();
        }

        static InetSocketAddress getAddress(RetainableByteBuffer msg) throws UnknownHostException {
            final int pos = msg.position();
            try {
                msg.position(pos + HEADER_SIZE);
                final int rawAddrLength = Short.toUnsignedInt(msg.getShort());
                final byte [] rawAddr = new byte[rawAddrLength];
                msg.get(rawAddr);
                final InetAddress addr = InetAddress.getByAddress(rawAddr);
                final int portNumber = Short.toUnsignedInt(msg.getShort());
                return new InetSocketAddress(addr, portNumber);
            } finally {
                msg.position(pos);
            }
        }
    }
}
