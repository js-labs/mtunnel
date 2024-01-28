package org.jsl.mtunnel;

import org.jsl.collider.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

public class Server {
    private final Collider m_collider;
    private final NetworkInterface m_networkInterface;

    private final ReentrantLock m_lock;
    private final HashMap<Channel, ClientInfo> m_clients;
    private final HashMap<InetSocketAddress, MulticastListener> m_multicastListeners;

    private static class ClientInfo {
        public final ArrayList<InetSocketAddress> groups;

        ClientInfo() {
            groups = new ArrayList<>();
        }
    }

    private class TunnelAcceptor extends Acceptor {
        private final TimerQueue m_timerQueue;
        private final int m_pingInterval;

        public TunnelAcceptor(int portNumber, TimerQueue timerQueue, int pingInterval) {
            super(portNumber);
            m_timerQueue = timerQueue;
            m_pingInterval = pingInterval;
        }

        @Override
        public Channel.Listener createSessionListener(Channel channel) {
            System.out.println("Client " + channel.getRemoteAddress() + " connected.");
            return new ServerChannel(channel, m_timerQueue, m_pingInterval);
        }

        @Override
        public void onAcceptorStarted(Collider collider, int localPort) {
            System.out.println("mTunnel server started at port " + localPort);
        }
    }

    private class ServerChannel extends TunnelChannel {
        public ServerChannel(Channel channel, TimerQueue timerQueue, int pingInterval) {
            super(channel, timerQueue, pingInterval);
        }

        @Override
        public void onConnectionClosed() {
            super.onConnectionClosed();
            System.out.println("Client " + m_channel.getRemoteAddress() + " disconnected");
            onClientDisconnected(m_channel);
        }

        @Override
        public void onMessageReceived(RetainableByteBuffer msg) {
            final short messageId = Protocol.Message.getMessageId(msg);
            if (messageId == Protocol.JoinRequest.ID) {
                try {
                    final InetSocketAddress[] groups = Protocol.JoinRequest.getGroups(msg);
                    joinGroups(m_channel, groups);
                }
                catch (UnknownHostException ex) {
                    System.out.println("Invalid message received from "
                            + m_channel.getRemoteAddress() + ", close connection");
                    m_channel.closeConnection();
                }
            }
        }
    }

    private class MulticastListener extends DatagramListener {
        public ArrayList<Channel> clients;

        public MulticastListener(InetSocketAddress addr, Channel channel) {
            super(addr);
            clients = new ArrayList<>();
            clients.add(channel);
        }

        @Override
        public void onDataReceived(RetainableByteBuffer data, SocketAddress sourceAddr) {
            Server.this.sendData(data, this);
        }
    }

    private void sendData(RetainableByteBuffer data, MulticastListener multicastListener) {
        ArrayList<Channel> clients;
        m_lock.lock();
        try {
            clients = multicastListener.clients;
        } finally {
            m_lock.unlock();
        }

        final ByteBuffer msg = Protocol.MulticastPacket.create(multicastListener.getAddr(), data);
        for (Channel clientChannel: clients) {
            clientChannel.sendData(msg);
        }
    }

    private void onClientDisconnected(Channel clientChannel) {
        final ArrayList<MulticastListener> groupsToLeave = new ArrayList<>();
        m_lock.lock();
        try {
            final ClientInfo clientInfo = m_clients.remove(clientChannel);
            if (clientInfo != null) {
                for (InetSocketAddress groupAddr: clientInfo.groups) {
                    final MulticastListener multicastListener = m_multicastListeners.get(groupAddr);
                    final ArrayList<Channel> clients = new ArrayList<>(multicastListener.clients);
                    clients.remove(clientChannel);
                    if (clients.isEmpty()) {
                        groupsToLeave.add(multicastListener);
                    } else {
                        multicastListener.clients = clients;
                    }
                }
            }
        }
        finally {
            m_lock.unlock();
        }

        boolean interrupted = false;
        for (MulticastListener multicastListener: groupsToLeave) {
            try {
                System.out.println("Leave multicast group " + multicastListener.getAddr());
                m_collider.removeDatagramListener(multicastListener);
            } catch (InterruptedException ex){
                System.out.println(ex.getMessage());
                interrupted = true;
            }
        }

        if (interrupted)
            Thread.currentThread().interrupt();
    }

    private void sendFailResponseAndCloseConnection(String msg, Channel clientChannel) {
        try {
            final ByteBuffer response = Protocol.JoinResponse.create(msg);
            clientChannel.sendData(response);
        } catch (Exception ex1) {
            System.out.println(ex1.getMessage());
        }
        clientChannel.closeConnection();
    }

    private void joinGroups(Channel clientChannel, InetSocketAddress [] groups) {
        final SocketAddress clientAddr = clientChannel.getRemoteAddress();
        for (final InetSocketAddress groupAddr : groups) {
            if (groupAddr.getAddress().isMulticastAddress()) {
                MulticastListener multicastListener;

                m_lock.lock();
                try {
                    multicastListener = m_multicastListeners.get(groupAddr);
                    if (multicastListener == null) {
                        multicastListener = new MulticastListener(groupAddr, clientChannel);
                        m_multicastListeners.put(groupAddr, multicastListener);
                    } else {
                        final ArrayList<Channel> clients = new ArrayList<>(multicastListener.clients);
                        clients.add(clientChannel);
                        multicastListener.clients = clients;
                        multicastListener = null;
                    }
                } finally {
                    m_lock.unlock();
                }

                if (multicastListener != null) {
                    try {
                        m_collider.addDatagramListener(multicastListener, m_networkInterface);
                        System.out.println("Joined multicast group " + groupAddr + " for client " + clientAddr);
                    } catch (IOException ex) {
                        System.out.println("Can't join multicast group " + groupAddr + " for client " + clientAddr + ": " + ex);
                        sendFailResponseAndCloseConnection(ex.getMessage(), clientChannel);
                        break;
                    }
                }

                m_lock.lock();
                try {
                    ClientInfo clientInfo = m_clients.get(clientChannel);
                    if (clientInfo == null) {
                        clientInfo = new ClientInfo();
                        m_clients.put(clientChannel, clientInfo);
                    }
                    clientInfo.groups.add(groupAddr);
                } finally {
                    m_lock.unlock();
                }
            } else {
                final String msg = groupAddr + "is not a multicast address";
                sendFailResponseAndCloseConnection(msg, clientChannel);
                break;
            }
        }
    }

    public Server(Collider collider, NetworkInterface networkInterface, int portNumber, TimerQueue timerQueue, int pingInterval) throws IOException {
        m_collider = collider;
        m_networkInterface = networkInterface;
        m_lock = new ReentrantLock();
        m_clients = new HashMap<>();
        m_multicastListeners = new HashMap<>();

        final Acceptor acceptor = new TunnelAcceptor(portNumber, timerQueue, pingInterval);
        collider.addAcceptor(acceptor);
    }
}
