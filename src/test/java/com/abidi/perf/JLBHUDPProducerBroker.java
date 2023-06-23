package com.abidi.perf;

import com.abidi.broker.UDPProducerBroker;
import com.abidi.marketdata.model.MarketData;
import com.abidi.queue.CircularMMFQueue;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.abidi.queue.CircularMMFQueue.getInstance;
import static java.lang.System.nanoTime;

public class JLBHUDPProducerBroker implements JLBHTask {

    private static final Logger LOG = LoggerFactory.getLogger(JLBHUDPProducerBroker.class);

    private JLBH jlbh;
    private UDPProducerBroker udpProducerBroker;
    private Thread producerThread;

    private int id;
    private double price = 0;


    public static void main(String[] args) {
        JLBHOptions jlbhOptions = new JLBHOptions()
                .warmUpIterations(10_000).iterations(5_000_000).throughput(1_000_000).runs(3).accountForCoordinatedOmission(false)
                .recordOSJitter(false).jlbhTask(new JLBHUDPProducerBroker());

        new JLBH(jlbhOptions).start();
    }

    @Override
    public void init(JLBH jlbh) {
        this.jlbh = jlbh;
        MarketData md = new MarketData();

        md.set("GB00BJLR0J16", 101.12d, 1, true, (byte) 1, "BRC", "2022-09-14:22:10:13", id);
        try {
            udpProducerBroker = new UDPProducerBroker(md.size());
            CircularMMFQueue circularMMFQueue = getInstance(md.size(), "/tmp/producer");
            producerThread = new Thread(() -> { 
                while (true) {
                    md.setPrice(++price);
                    md.setId(id++);
                    circularMMFQueue.add(md.getData());
                }
            });
            producerThread.start();

        } catch (IOException exp) {
            LOG.error("Failed to init jlbh test", exp);
        }
    }


    @Override
    public void run(long startTimeNS) {
        udpProducerBroker.sendNext();
        jlbh.sampleNanos((nanoTime() - 10) - startTimeNS);
    }
}
