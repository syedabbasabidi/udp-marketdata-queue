package com.abidi.consumer;

import com.abidi.marketdata.model.MarketDataCons;
import com.abidi.producer.UDPQueueProducer;
import com.abidi.queue.CircularMMFQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class UDPQueueConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(UDPQueueProducer.class);
    private static final MarketDataCons marketData = new MarketDataCons();
    private long msgCount = 0;

    public static final int QUEUE_SIZE = 2000_000;

    public static void main(String[] args) throws IOException {
        CircularMMFQueue mmfQueue = new CircularMMFQueue(marketData.size(), QUEUE_SIZE, "/tmp");
        UDPQueueConsumer udpQueueConsumer = new UDPQueueConsumer();
        udpQueueConsumer.run(mmfQueue);
    }

    public void run(CircularMMFQueue mmfQueue) {

        LOG.info("Reading to consume");
        while (true) {
            byte[] bytes = mmfQueue.get();
            if (bytes != null) {
                msgCount++;//process(marketData, bytes);
                if (msgCount == 1) LOG.info("First msg arrived");
            }

            if (msgCount >= 1_000_000) {
                LOG.info("All messages consumed");
                break;
            }
        }
    }


    private static void process(MarketDataCons marketData, byte[] data) {
        marketData.setData(data);
        LOG.debug("Message received {}", marketData);
    }
}
