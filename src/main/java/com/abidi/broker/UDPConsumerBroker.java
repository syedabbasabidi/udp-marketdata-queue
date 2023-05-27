package com.abidi.broker;

import com.abidi.marketdata.model.MarketDataCons;
import com.abidi.queue.CircularMMFQueue;
import com.abidi.util.ByteUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

import static com.abidi.consumer.UDPQueueConsumer.QUEUE_SIZE;
import static java.net.InetAddress.getLocalHost;

public class UDPConsumerBroker {

    private static final Logger LOG = LoggerFactory.getLogger(UDPConsumerBroker.class);
    private final CircularMMFQueue circularMMFQueue;

    private final DatagramSocket socket;
    private volatile DatagramPacket packet;
    private final byte[] bytes;
    private final byte[] ackMsgSeq = new byte[8];

    private final DatagramPacket ackPacket;

    private MarketDataCons marketDataCons = new MarketDataCons();


    private long msgCount = 0;


    public static void main(String[] args) throws IOException {
        MarketDataCons marketDataCons = new MarketDataCons();
        UDPConsumerBroker udpConsumerBroker = new UDPConsumerBroker(marketDataCons.size());
        udpConsumerBroker.process();
    }

    public UDPConsumerBroker(int msgSize) throws IOException {

        this.circularMMFQueue = new CircularMMFQueue(msgSize, QUEUE_SIZE, "/tmp");
        socket = new DatagramSocket(5000, InetAddress.getLocalHost());
        bytes = new byte[msgSize];
        packet = new DatagramPacket(bytes, msgSize, InetAddress.getLocalHost(), 5001);
        ackPacket = new DatagramPacket(ackMsgSeq, 8, getLocalHost(), 5001);


    }

    private void process() {

        while (true) {
            try {
                socket.receive(packet);
                if (packet.getData() != null && packet.getData().length > 10) {
                    marketDataCons.setData(packet.getData());
                    ackPacket.setData(ByteUtils.longToBytes(marketDataCons.getId()));

                    if (circularMMFQueue.add(packet.getData())) {
                        LOG.info("{} Msg enqueued {}, sending ack", ++msgCount, marketDataCons);
                        socket.send(ackPacket);
                    } else {
                        LOG.info("Can't accept msg, queue is full");
                    }
                }
            } catch (Exception exp) {
                LOG.error("Failed to receive msg");
            }
        }
    }
}
