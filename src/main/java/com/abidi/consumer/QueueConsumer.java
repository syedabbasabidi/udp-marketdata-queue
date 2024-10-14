package com.abidi.consumer;

import com.abidi.marketdata.model.MarketDataCons;
import com.abidi.producer.QueueProducer;
import com.abidi.queue.CircularMMFQueue;
import com.abidi.util.ByteUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.abidi.constants.Config.UNDERLYING_CONSUMER_QUEUE_PATH;

public class QueueConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(QueueProducer.class);
    public static final int QUEUE_SIZE = 1_000_000;
    private final MarketDataCons marketData;
    private final CircularMMFQueue mmfQueue;

    public QueueConsumer() throws IOException {
        ByteUtils byteUtils = new ByteUtils();
        marketData = new MarketDataCons(byteUtils);
        mmfQueue = new CircularMMFQueue(marketData.size(), QUEUE_SIZE, UNDERLYING_CONSUMER_QUEUE_PATH);
    }

    public static void main(String[] args) throws IOException {
        LOG.info("Starting Market Data Consumer...");
        QueueConsumer queueConsumer = new QueueConsumer();
        queueConsumer.consume();
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
