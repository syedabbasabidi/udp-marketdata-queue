package com.abidi.broker;

import com.abidi.queue.CircularMMFQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class UDPConsumerBroker {

    private static final Logger LOG = LoggerFactory.getLogger(UDPConsumerBroker.class);
    private final ExecutorService executorService = Executors.newSingleThreadExecutor(new BrokerThreadFactory());
    private final CircularMMFQueue circularMMFQueue;

    private final DatagramSocket socket;
    private volatile DatagramPacket packet;

    private final byte[] bytes;


    public UDPConsumerBroker(CircularMMFQueue circularMMFQueue, int msgSize) throws SocketException {
        this.circularMMFQueue = circularMMFQueue;
        socket = new DatagramSocket();
        bytes = new byte[msgSize];
        packet = new DatagramPacket(bytes, msgSize);

    }

    public void start() {
        executorService.submit(this::process);
    }

    private void process() {

        while (true) {
            try {
                socket.receive(packet);
                if (packet.getData() != null && packet.getData().length > 10)
                    circularMMFQueue.add(packet.getData());
            } catch (Exception exp) {
                LOG.error("Failed to receive msg");
            }
        }
    }

    static class BrokerThreadFactory implements ThreadFactory {
        public Thread newThread(Runnable r) {
            return new Thread(r, UDPConsumerBroker.class.getName());
        }
    }
}
