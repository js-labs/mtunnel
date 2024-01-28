package org.jsl.mtunnel;

import org.jsl.collider.*;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;

public class Client {
    private static class TunnelConnector extends Connector {
        private final Collider m_collider;
        private final TimerQueue m_timerQueue;
        private final int m_pingInterval;
        private final ByteBuffer m_joinRequest;

        public TunnelConnector(InetSocketAddress addr, Collider collider, TimerQueue timerQueue, int pingInterval, ByteBuffer joinRequest) {
            super(addr);
            m_collider = collider;
            m_timerQueue = timerQueue;
            m_pingInterval = pingInterval;
            m_joinRequest = joinRequest;
        }

        @Override
        public Channel.Listener createSessionListener(Channel channel) {
            System.out.println("Connected to server @ " + channel.getRemoteAddress());
            try {
                return new ClientChannel(channel, m_timerQueue, m_pingInterval, m_joinRequest);
            } catch (SocketException ex) {
                System.out.println(ex.getMessage());
                m_collider.stop();
                return null;
            }
        }

        @Override
        public void onException(IOException ex) {
            System.out.println(ex.getMessage());
            m_collider.stop();
        }
    }

    private static class ClientChannel extends TunnelChannel {
        private final DatagramSocket m_dataframSocket;

        public ClientChannel(Channel channel, TimerQueue timerQueue, int pingInterval, ByteBuffer joinRequest) throws SocketException {
            super(channel, timerQueue, pingInterval);
            m_dataframSocket = new DatagramSocket();
            channel.sendData(joinRequest);
        }

        @Override
        public void onConnectionClosed() {
            super.onConnectionClosed();
            System.out.println("Connection to server " + m_channel.getRemoteAddress() + " lost.");
            m_channel.getCollider().stop();
        }

        @Override
        public void onMessageReceived(RetainableByteBuffer msg) {
            final short messageId = Protocol.Message.getMessageId(msg);
            if (messageId == Protocol.JoinResponse.ID) {
                try {
                    final String statusText = Protocol.JoinResponse.getStatusText(msg);
                    if (statusText != null) {
                        System.out.println(statusText);
                        m_channel.getCollider().stop();
                    }
                } catch (final CharacterCodingException ex) {
                    System.out.println("Can't decode server response: " + ex.getMessage());
                    m_channel.getCollider().stop();
                }
            }
            else if (messageId == Protocol.MulticastPacket.ID) {
            }
            else {
                super.onMessageReceived(msg);
            }
        }
    }

    public Client(Collider collider, InetSocketAddress address, TimerQueue timerQueue, int pingInterval, ByteBuffer joinRequest) {
        final Connector connector = new TunnelConnector(address, collider, timerQueue, pingInterval, joinRequest);
        collider.addConnector(connector);
    }
}
