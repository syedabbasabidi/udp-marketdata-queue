package com.abidi.broker.udp;

import com.abidi.marketdata.model.MarketDataCons;
import com.abidi.queue.CircularMMFQueue;
import com.abidi.util.ByteUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;

import static com.abidi.constants.Config.*;
import static com.abidi.consumer.QueueConsumer.QUEUE_SIZE;
import static java.net.InetAddress.getLocalHost;
import static org.slf4j.LoggerFactory.getLogger;

public class UDPProducerBroker {

    private static final Logger LOG = getLogger(UDPProducerBroker.class);
    public static final int SO_TIMEOUT = 5000;
    private final CircularMMFQueue circularMMFQueue;
    private final DatagramSocket socket;
    private final DatagramPacket ackPacket;
    private final DatagramPacket msgPacket;
    private final ByteUtils byteUtils = new ByteUtils();
    private final MarketDataCons marketDataCons;

    public UDPProducerBroker() throws IOException {
        marketDataCons = new MarketDataCons(byteUtils);
        this.circularMMFQueue = new CircularMMFQueue(marketDataCons.size(), QUEUE_SIZE, UNDERLYING_PRODUCER_QUEUE_PATH);
        socket = new DatagramSocket(UDP_PROD_BROKER_SOCKET_PORT);
        socket.setSoTimeout(SO_TIMEOUT);
        byte[] bytes = new byte[marketDataCons.size()];
        msgPacket = new DatagramPacket(bytes, marketDataCons.size(), getLocalHost(), UDP_CON_BROKER_SOCKET_PORT);
        byte[] ackMsgSeq = new byte[8];
        ackPacket = new DatagramPacket(ackMsgSeq, 8, getLocalHost(), UDP_CON_BROKER_SOCKET_PORT);
    }

    public static void main(String[] args) throws IOException {
        UDPProducerBroker udpProducerBroker = new UDPProducerBroker();
        udpProducerBroker.startBroker();
    }

    public void startBroker() {
        while (true) {
            sendNext();
        }
    }

    public void sendNext() {
        byte[] bytes = circularMMFQueue.getWithAck();
        if (bytes != null) sendItAcross(bytes);
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
        return byteUtils.longToBytes(marketDataCons.getId());
    }

    private long idFromAck() {
        return byteUtils.bytesToLong(ackPacket.getData(), 0, 8);
    }
}