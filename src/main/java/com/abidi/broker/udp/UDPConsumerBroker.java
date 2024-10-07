package com.abidi.broker.udp;

import com.abidi.marketdata.model.MarketDataCons;
import com.abidi.queue.CircularMMFQueue;
import com.abidi.util.ByteUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

import static com.abidi.constants.Config.*;
import static com.abidi.consumer.QueueConsumer.QUEUE_SIZE;
import static java.net.InetAddress.getLocalHost;

public class UDPConsumerBroker {

    private static final Logger LOG = LoggerFactory.getLogger(UDPConsumerBroker.class);
    private final CircularMMFQueue circularMMFQueue;
    private final DatagramSocket socket;
    private final DatagramPacket mdPacket;
    private final DatagramPacket ackPacket;
    private final MarketDataCons marketDataCons;
    private long msgCount = 0;
    private final ByteUtils byteUtils = new ByteUtils();

    public UDPConsumerBroker() throws IOException {

        marketDataCons = new MarketDataCons(byteUtils);
        this.circularMMFQueue = new CircularMMFQueue(marketDataCons.size(), QUEUE_SIZE, UNDERLYING_CONSUMER_QUEUE_PATH);
        socket = new DatagramSocket(UDP_CON_BROKER_SOCKET_PORT, InetAddress.getLocalHost());

        byte[] bytes = new byte[marketDataCons.size()];
        mdPacket = new DatagramPacket(bytes, marketDataCons.size());
        byte[] ackMsgSeq = new byte[8];
        ackPacket = new DatagramPacket(ackMsgSeq, ackMsgSeq.length, getLocalHost(), UDP_PROD_BROKER_SOCKET_PORT);
    }

    public static void main(String[] args) throws IOException {
        UDPConsumerBroker udpConsumerBroker = new UDPConsumerBroker();
        udpConsumerBroker.startBroker();
    }

    public void startBroker() {

        while (true) {
            getNextAndAck();
        }
    }

    public void getNextAndAck() {
        try {
            socket.receive(mdPacket);
            if (mdPacket.getData() != null) {
                marketDataCons.setData(mdPacket.getData());
                ackPacket.setData(byteUtils.longToBytes(marketDataCons.getId()));

                if (circularMMFQueue.add(mdPacket.getData())) {
                    LOG.info("{} Msg enqueued {}, sending ack", ++msgCount, marketDataCons);
                    socket.send(ackPacket);
                } else {
                    LOG.info("Can't accept msg, queue is full");
                }
            }
        } catch (Exception exp) {
            LOG.error("Failed to receive msg", exp);
        }
    }
}
