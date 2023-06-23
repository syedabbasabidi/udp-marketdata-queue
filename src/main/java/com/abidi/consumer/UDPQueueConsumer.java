package com.abidi.consumer;

import com.abidi.marketdata.model.MarketDataCons;
import com.abidi.producer.UDPQueueProducer;
import com.abidi.queue.CircularMMFQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class UDPQueueConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(UDPQueueProducer.class);
    public static final int QUEUE_SIZE = 1_000_000;
    private final MarketDataCons marketData;
    private final CircularMMFQueue mmfQueue;

    public UDPQueueConsumer() throws IOException {
        marketData = new MarketDataCons();
        mmfQueue = new CircularMMFQueue(marketData.size(), QUEUE_SIZE, "/tmp/consumer");
    }

    public static void main(String[] args) throws IOException {
        LOG.info("Starting Market Data Consumer...");
        UDPQueueConsumer udpQueueConsumer = new UDPQueueConsumer();
        udpQueueConsumer.consume();
    }

    public void consume() {

        LOG.info("Reading to consume");
        while (true) {
            byte[] bytes = mmfQueue.get();
            if (bytes != null) {
                marketData.setData(bytes);
                LOG.debug("Message received {}", marketData);
            }
        }
    }
}
