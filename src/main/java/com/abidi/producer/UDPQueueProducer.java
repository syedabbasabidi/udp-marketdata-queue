package com.abidi.producer;

import com.abidi.marketdata.model.MarketData;
import com.abidi.queue.CircularMMFQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class UDPQueueProducer {

    private static final Logger LOG = LoggerFactory.getLogger(UDPQueueProducer.class);

    public static void main(String[] args) throws IOException {

        LOG.info("Starting Market Data Generator...");
        MarketData md = new MarketData();
        CircularMMFQueue mmfQueue = new CircularMMFQueue(md.size(), 10, "/home/mesum");
        mmfQueue.reset();

        int j = 0;
        md.set("GB00BJLR0J16", 1d + j, 0, true, (byte) 1, "BRC", "2023-02-14:22:10:13", j);
        while (true) {
            md.setPrice(1d + j);
            md.side(j % 2 == 0 ? 0 : 1);
            md.setFirm(j % 2 == 0);
            md.setId(j);
            boolean added = mmfQueue.add(md.getData());
            j = added ? j + 1 : j;
            if (added)
                LOG.info("Adding to queue {}", j);

        }
    }
}
