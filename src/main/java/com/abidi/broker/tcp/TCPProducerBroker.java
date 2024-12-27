package com.abidi.broker.tcp;

import com.abidi.constants.Config;
import com.abidi.marketdata.model.MarketDataCons;
import com.abidi.queue.CircularMMFQueue;
import com.abidi.util.ByteUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import static com.abidi.constants.Config.UNDERLYING_PRODUCER_QUEUE_PATH;
import static com.abidi.consumer.QueueConsumer.QUEUE_SIZE;
import static org.slf4j.LoggerFactory.getLogger;

public class TCPProducerBroker {

    private static final Logger LOG = getLogger(TCPProducerBroker.class);
    private final CircularMMFQueue circularMMFQueue;
    private final ServerSocket serverSocket;
    private final byte[] ackbytes;
    private final ByteUtils byteUtils = new ByteUtils();
    private volatile Socket clientSocket;
    private final MarketDataCons marketDataCons;

    public TCPProducerBroker() throws IOException {
        marketDataCons = new MarketDataCons(byteUtils);
        this.circularMMFQueue = new CircularMMFQueue(marketDataCons.size(), QUEUE_SIZE, UNDERLYING_PRODUCER_QUEUE_PATH);
        ackbytes = new byte[marketDataCons.size()];
        serverSocket = new ServerSocket(Config.TCP_BROKER_SERVER_SOCKET_PORT, 10, InetAddress.getByName("192.168.1.52"));
        LOG.info("Server started... {}", serverSocket.getInetAddress());
    }

    public static void main(String[] args) throws IOException {
        TCPProducerBroker udpProducerBroker = new TCPProducerBroker();
        udpProducerBroker.init();
    }

    public void initSynchronously() throws IOException {
        startServer();
    }

    public void init() throws IOException {
        new Thread(() -> {
            startServer();
            startBroker();
        }).start();
    }

    private void startServer() {
        try {
            clientSocket = serverSocket.accept();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        LOG.info("Consumer broker connected!");
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

        marketDataCons.setData(bytes);

        while (true) {
            try {
                LOG.info("Sending data {}", marketDataCons);
                clientSocket.getOutputStream().write(bytes);
                clientSocket.getInputStream().read(ackbytes);

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

    private long idFromAck() {
        return byteUtils.bytesToLong(ackbytes, 0, 8);
    }


}