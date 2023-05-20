package com.abidi.broker;

import com.abidi.queue.CircularMMFQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class UDPProducerBroker {

    private static final Logger LOG = LoggerFactory.getLogger(UDPProducerBroker.class);
    private final ExecutorService executorService = Executors.newSingleThreadExecutor(new BrokerThreadFactory());
    private final CircularMMFQueue circularMMFQueue;

    private final DatagramSocket socket;
    private volatile DatagramPacket packet;

    private final byte[] bytes;


    public UDPProducerBroker(CircularMMFQueue circularMMFQueue, int msgSize) throws SocketException, UnknownHostException {
        this.circularMMFQueue = circularMMFQueue;
        socket = new DatagramSocket(5001);
        bytes = new byte[msgSize];
        packet = new DatagramPacket(bytes, msgSize, InetAddress.getLocalHost(), 5000);

    }

    public void start() {
        executorService.submit(this::process);
    }

    private void process() {

        while (true) {
            byte[] bytes = circularMMFQueue.get();
            if (bytes != null) sendItAcross(bytes);
        }
    }

    private void sendItAcross(byte[] bytes) {
        packet.setData(bytes);
        try {
            socket.send(packet);
        } catch (Exception exp) {
            LOG.error("Failed to send msg", exp);
        }
    }

    static class BrokerThreadFactory implements ThreadFactory {
        public Thread newThread(Runnable r) {
            return new Thread(r, UDPProducerBroker.class.getName());
        }
    }
}
