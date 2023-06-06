package com.abidi.broker;

import com.abidi.marketdata.model.MarketData;
import com.abidi.marketdata.model.MarketDataCons;
import com.abidi.queue.CircularMMFQueue;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;

import static com.abidi.consumer.UDPQueueConsumer.QUEUE_SIZE;
import static com.abidi.util.ByteUtils.bytesToLong;
import static com.abidi.util.ByteUtils.longToBytes;
import static java.net.InetAddress.getLocalHost;
import static org.slf4j.LoggerFactory.getLogger;

public class UDPProducerBroker {

    private static final Logger LOG = getLogger(UDPProducerBroker.class);
    private final CircularMMFQueue circularMMFQueue;

    private final DatagramSocket socket;
    private final DatagramPacket ackPacket;
    private volatile DatagramPacket msgPacket;

    private final byte[] bytes;
    private final byte[] ackMsgSeq = new byte[8];
    private MarketDataCons marketDataCons = new MarketDataCons();


    public static void main(String[] args) throws IOException {
        MarketData marketData = new MarketData();
        UDPProducerBroker udpProducerBroker = new UDPProducerBroker(marketData.size());
        udpProducerBroker.process();
    }

    public UDPProducerBroker(int msgSize) throws IOException {
        this.circularMMFQueue = new CircularMMFQueue(msgSize, QUEUE_SIZE, "/tmp/producer");
        socket = new DatagramSocket(5001);
        socket.setSoTimeout(5000);
        bytes = new byte[msgSize];
        msgPacket = new DatagramPacket(bytes, msgSize, getLocalHost(), 5000);
        ackPacket = new DatagramPacket(ackMsgSeq, 8, getLocalHost(), 5000);
    }

    private void process() {

        while (true) {
            byte[] bytes = circularMMFQueue.getWithoutAck();
            if (bytes != null) sendItAcross(bytes);
        }
    }

    private void sendItAcross(byte[] bytes) {

        msgPacket.setData(bytes);
        marketDataCons.setData(bytes);
        ackPacket.setData(extractMsgId());

        while (true) {
            try {

                LOG.info("Sending data {}", marketDataCons);
                socket.send(msgPacket);
                socket.receive(ackPacket);
                if (idFromAck() == marketDataCons.getId()) {
                    LOG.info("Ack for {} is received", marketDataCons.getId());
                    circularMMFQueue.ack();
                    break;
                }

            } catch (IOException exp) {
                LOG.info("Failed to send msg {}, will retry", marketDataCons.getId());
            }
        }
    }

    private byte[] extractMsgId() {
        return longToBytes(marketDataCons.getId());
    }

    private long idFromAck() {
        return bytesToLong(ackPacket.getData(), 0, 8);
    }
}