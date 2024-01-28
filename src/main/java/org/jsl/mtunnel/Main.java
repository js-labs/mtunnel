package org.jsl.mtunnel;

import org.jsl.collider.Collider;
import org.jsl.collider.TimerQueue;
import org.jsl.collider.Util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class Main {
    private static void printUsage() {
        System.out.println("Usage: mtunnel -s <server address|server port>");
    }

    private static void printErrorAndUsage(String err) {
        System.out.println("Error: " + err);
        printUsage();
    }

    private static InetSocketAddress parseAddress(String str) throws Exception {
        final String [] parts = str.split(":");
        if (parts.length == 2) {
            try {
                final InetAddress address = InetAddress.getByName(parts[0]);
                final int port = Integer.parseInt(parts[1]);
                return new InetSocketAddress(address, port);
            }
            catch (UnknownHostException ex) {
                throw new Exception("failed to resolve host '" + parts[0] + "': " + ex.toString());
            }
            catch (NumberFormatException ex) {
                throw new Exception("invalid server port: '" + parts[1] + "': " + ex.toString());
            }
        }
        else {
            return null;
        }
    }

    public static void main(String [] args) {
        int idx = 0;
        InetAddress serverAddress = null;
        final ArrayList<InetSocketAddress> groups = new ArrayList<InetSocketAddress>();
        int serverPort = 0;
        String networkInterfaceName = null;

        while (idx < args.length) {
            final String opt = args[idx];
            if (opt.equals("-s")) {
                if (++idx == args.length) {
                    printErrorAndUsage("missing server port number");
                    return;
                }

                InetSocketAddress addr;
                try {
                    addr = parseAddress(args[idx]);
                } catch (Exception ex) {
                    printErrorAndUsage(ex.getMessage());
                    return;
                }

                if (addr == null) {
                    try {
                        serverPort = Integer.parseInt(args[idx]);
                    } catch (NumberFormatException ex) {
                        printErrorAndUsage("invalid server port: '" + args[idx] + "': " + ex.toString());
                        return;
                    }
                } else {
                    serverAddress = addr.getAddress();
                    serverPort = addr.getPort();
                }
            } else if (opt.equals("-g")) {
                if (++idx == args.length) {
                    printErrorAndUsage("missing group address");
                    return;
                }

                try {
                    groups.add(parseAddress(args[idx]));
                } catch (Exception ex) {
                    printErrorAndUsage(ex.getMessage());
                    return;
                }
            } else if (opt.equals("-i")) {
                if (++idx == args.length) {
                    printErrorAndUsage("missing interface name");
                    return;
                }
                networkInterfaceName = args[idx];
            } else {
                printErrorAndUsage("invalid command line option '" + opt + "'");
                return;
            }
            idx++;
        }

        if (serverPort == 0) {
            printErrorAndUsage("missing server port or address.");
            return;
        }

        try {
            final Collider.Config colliderConfig = new Collider.Config();
            colliderConfig.threadPoolThreads = 2;
            colliderConfig.byteOrder = Protocol.BYTE_ORDER;

            final Collider collider = Collider.create(colliderConfig);
            final TimerQueue timerQueue = new TimerQueue(collider.getThreadPool());
            final int pingInterval = 5;

            if (serverAddress == null) {
                if (networkInterfaceName == null) {
                    System.out.println("Missing network interface name");
                    printUsage();
                    return;
                } else {
                    InetAddress networkInterfaceAddr = null;
                    try {
                        networkInterfaceAddr = InetAddress.getByName(networkInterfaceName);
                    } catch (UnknownHostException ignored){
                    }
                    final NetworkInterface networkInterface = (networkInterfaceAddr == null)
                            ? NetworkInterface.getByName(networkInterfaceName)
                            : NetworkInterface.getByInetAddress(networkInterfaceAddr);
                    new Server(collider, networkInterface, serverPort, timerQueue, pingInterval);
                }
            }  else {
                if (groups.isEmpty()) {
                    System.out.println("No one multicast group configured, stop.");
                    printUsage();
                    return;
                }

                try {
                    final InetSocketAddress addr = new InetSocketAddress(serverAddress, serverPort);
                    final ByteBuffer joinRequest = Protocol.JoinRequest.create(groups);
                    System.out.println(Util.hexDump(joinRequest));
                    new Client(collider, addr, timerQueue, pingInterval, joinRequest);
                }
                catch (Exception ex) {
                    System.out.println(ex.getMessage());
                    return;
                }
            }

            collider.run();
        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
