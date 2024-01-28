package org.jsl.mtunnel;

import org.jsl.collider.*;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class TunnelChannel implements Channel.Listener {
    private class TimerHandler implements TimerQueue.Task {
        private final long m_interval;

        TimerHandler(long interval) {
            m_interval = interval;
        }

        public long run() {
            sendPing();
            return m_interval;
        }
    }

    private void sendPing() {
        final ByteBuffer ping = m_ping.duplicate();
        m_channel.sendData(ping);
    }

    protected final Channel m_channel;
    private final StreamDefragger m_streamDefragger;
    private ByteBuffer m_ping;
    private ByteBuffer m_pong;
    private TimerQueue m_timerQueue;
    private TimerHandler m_timerHandler;

    public TunnelChannel(Channel channel, TimerQueue timerQueue, int pingInterval) {
        m_channel = channel;
        m_streamDefragger = new StreamDefragger(Short.SIZE/Byte.SIZE) {
            @Override
            protected int validateHeader(ByteBuffer header) {
                return Protocol.Message.getLength(header);
            }
        };

        if (pingInterval > 0) {
            m_ping = Protocol.Ping.create();
            m_pong = Protocol.Pong.create();
            m_timerQueue = timerQueue;
            m_timerHandler = new TimerHandler(TimeUnit.SECONDS.toMillis(pingInterval));
            m_ping = Protocol.Ping.create();
            timerQueue.schedule(m_timerHandler, pingInterval, TimeUnit.SECONDS);
        }
    }

    @Override
    public void onDataReceived(RetainableByteBuffer data) {
        RetainableByteBuffer msg = m_streamDefragger.getNext(data);
        while (msg != null) {
            if (msg == StreamDefragger.INVALID_HEADER) {
                System.out.println("Invalid message header received from "
                        + m_channel.getRemoteAddress() + ", close connection");
                m_channel.closeConnection();
                break;
            } else {
                onMessageReceived(msg);
                msg = m_streamDefragger.getNext();
            }
        }
    }

    @Override
    public void onConnectionClosed() {
        if (m_timerHandler != null) {
            boolean interrupted = false;
            try {
                m_timerQueue.cancel(m_timerHandler);
            } catch (final InterruptedException ex) {
                interrupted = true;
            }
            m_timerHandler = null;
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void onMessageReceived(RetainableByteBuffer msg) {
        final short messageId = Protocol.Message.getMessageId(msg);
        if (messageId == Protocol.Ping.ID) {
            m_channel.sendData(m_pong.duplicate());
        }
        else {
            System.out.println("Invalid message '" + messageId + "' received from "
                    + m_channel.getRemoteAddress() + ", close connection");
            m_channel.closeConnection();
        }
    }
}
