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

    public static void main(String[] args) throws IOException {
        CircularMMFQueue mmfQueue = new CircularMMFQueue(marketData.size(), 10, "/tmp");
        UDPQueueConsumer udpQueueConsumer = new UDPQueueConsumer();
        udpQueueConsumer.run(mmfQueue);
    }

    public void run(CircularMMFQueue mmfQueue) {

        LOG.info("Reading to consume");
        while (true) {
            byte[] bytes = mmfQueue.get();
            if (bytes != null) process(marketData, bytes);
        }
    }


    private static void process(MarketDataCons marketData, byte[] data) {
        marketData.setData(data);
        LOG.info("Message received {}", marketData);
    }
}
