package com.abidi.broker;

import com.abidi.marketdata.model.MarketDataCons;
import com.abidi.queue.CircularMMFQueue;
import com.abidi.util.ByteUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;

import static com.abidi.consumer.QueueConsumer.QUEUE_SIZE;

public class TCPConsumerBroker {

    private static final Logger LOG = LoggerFactory.getLogger(TCPConsumerBroker.class);
    private final CircularMMFQueue circularMMFQueue;
    private final Socket socket;
    private final byte[] msgBytes;
    private final MarketDataCons marketDataCons;
    private final InputStream inputStream;
    private final OutputStream outputStream;
    private long msgCount = 0;
    private final ByteUtils byteUtils = new ByteUtils();


    public TCPConsumerBroker() throws IOException {

        marketDataCons = new MarketDataCons(byteUtils);
        this.circularMMFQueue = new CircularMMFQueue(marketDataCons.size(), QUEUE_SIZE, "/tmp/consumer");
        socket = new Socket(InetAddress.getLocalHost(), 5001);
        inputStream = socket.getInputStream();
        outputStream = socket.getOutputStream();
        msgBytes = new byte[marketDataCons.size()];
    }

    public static void main(String[] args) throws IOException {
        TCPConsumerBroker udpConsumerBroker = new TCPConsumerBroker();
        udpConsumerBroker.startBroker();
    }

    public void startBroker() {

        while (true) {
            getNextAndAck();
        }
    }

    public void getNextAndAck() {
        try {

            int bytesRead = inputStream.read(msgBytes);
            if (bytesRead == marketDataCons.size()) {
                marketDataCons.setData(msgBytes);
                if (circularMMFQueue.add(msgBytes)) {
                    LOG.info("{} Msg enqueued {}, sending ack", ++msgCount, marketDataCons);
                    outputStream.write(byteUtils.longToBytes(marketDataCons.getId()));
                } else {
                    LOG.info("Can't accept msg, queue is full");
                }
            }
        } catch (Exception exp) {
            LOG.error("Failed to receive msg", exp);
        }
    }
}
