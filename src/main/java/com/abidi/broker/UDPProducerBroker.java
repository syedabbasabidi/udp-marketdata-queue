package com.abidi.broker;

import com.abidi.marketdata.model.MarketData;
import com.abidi.marketdata.model.MarketDataCons;
import com.abidi.queue.CircularMMFQueue;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketTimeoutException;

import static com.abidi.util.ByteUtils.bytesToLong;
import static com.abidi.util.ByteUtils.longToBytes;
import static java.net.InetAddress.getLocalHost;
import static org.slf4j.LoggerFactory.getLogger;

public class UDPProducerBroker {

    private static final Logger LOG = getLogger(UDPProducerBroker.class);
    private final CircularMMFQueue circularMMFQueue;

    private final DatagramSocket socket;
    private final DatagramPacket ackPacket;
    private volatile DatagramPacket packet;

    private final byte[] bytes;
    private final byte[] ackMsgSeq = new byte[8];

    private long seq = 0;

    public static void main(String[] args) throws IOException {
        MarketData marketData = new MarketData();
        UDPProducerBroker udpProducerBroker = new UDPProducerBroker(marketData.size());
        udpProducerBroker.process();
    }

    public UDPProducerBroker(int msgSize) throws IOException {
        this.circularMMFQueue = new CircularMMFQueue(msgSize, 10, "/home/mesum");
        socket = new DatagramSocket(5001);
        socket.setSoTimeout(5000);
        bytes = new byte[msgSize];
        packet = new DatagramPacket(bytes, msgSize, getLocalHost(), 5000);
        ackPacket = new DatagramPacket(ackMsgSeq, 8, getLocalHost(), 5000);
    }

    private void process() {

        while (true) {
            byte[] bytes = circularMMFQueue.get();
            if (bytes != null) sendItAcross(bytes);
        }
    }

    private void sendItAcross(byte[] bytes) {
        packet.setData(bytes);
        MarketDataCons marketDataCons = new MarketDataCons();
        marketDataCons.setData(bytes);
        ackPacket.setData(longToBytes(marketDataCons.getId()));

        while (true) {
            try {

                LOG.info("Sending data {}", marketDataCons);
                socket.send(packet);
                socket.receive(ackPacket);
                if (bytesToLong(ackPacket.getData(), 0, 8) == marketDataCons.getId()) {
                    break;
                }

            } catch (SocketTimeoutException exp) {
                LOG.info("Failed to send seq {}, will retry", seq);
            } catch (Exception exp) {
                LOG.error("Failed to send msg", exp);
            }
        }
    }
}