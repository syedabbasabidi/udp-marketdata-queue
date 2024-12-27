package com.abidi.producer;

import com.abidi.marketdata.model.MarketData;
import com.abidi.queue.CircularMMFQueue;
import com.abidi.util.ByteUtils;
import com.abidi.util.ChecksumUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.abidi.constants.Config.UNDERLYING_PRODUCER_QUEUE_PATH;
import static com.abidi.consumer.QueueConsumer.QUEUE_SIZE;

public class QueueProducer {

    private static final Logger LOG = LoggerFactory.getLogger(QueueProducer.class);
    private final CircularMMFQueue mmfQueue;
    private final MarketData md;

    public QueueProducer() throws IOException {
        ByteUtils byteUtils = new ByteUtils();
        md = new MarketData(byteUtils, new ChecksumUtil());
        mmfQueue = new CircularMMFQueue(md.size(), QUEUE_SIZE, UNDERLYING_PRODUCER_QUEUE_PATH);
    }

    public static void main(String[] args) throws IOException {

        LOG.info("Starting Market Data Generator...");
        QueueProducer queueProducer = new QueueProducer();
        queueProducer.produce();
    }

    public void produce() {
        LOG.info("Sending MD messages...");
        int msgId = 1;
        md.set("GB00BJLR0J16", 1d + msgId, 0, true, (byte) 1, "BRC", "2023-02-14:22:10:13", msgId);
        while (true) {
            md.setPrice(1d + msgId);
            md.side(msgId % 2 == 0 ? 0 : 1);
            md.setFirm(msgId % 2 == 0);
            md.setId(msgId);
            if (mmfQueue.add(md.getData())) {
                if(LOG.isDebugEnabled()) {
                    LOG.debug("Msg {} sent", msgId);
                }
                msgId++;
            }
        }
    }
}