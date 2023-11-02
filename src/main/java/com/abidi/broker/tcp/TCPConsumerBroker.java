package com.abidi.broker.tcp;

import com.abidi.constants.Config;
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

import static com.abidi.constants.Config.UNDERLYING_CONSUMER_QUEUE_PATH;
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
        this.circularMMFQueue = new CircularMMFQueue(marketDataCons.size(), QUEUE_SIZE, UNDERLYING_CONSUMER_QUEUE_PATH);
        socket = new Socket(InetAddress.getLocalHost(), Config.TCP_BROKER_SERVER_SOCKET_PORT, InetAddress.getLocalHost(), Config.TCP_BROKER_CLIENT_SOCKET_PORT);
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
