package com.abidi.perf;

import com.abidi.broker.TCPConsumerBroker;
import com.abidi.broker.TCPProducerBroker;
import com.abidi.consumer.QueueConsumer;
import com.abidi.producer.QueueProducer;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static java.lang.System.nanoTime;

public class JLBHTCPProducerBroker implements JLBHTask {

    private static final Logger LOG = LoggerFactory.getLogger(JLBHTCPProducerBroker.class);

    private JLBH jlbh;
    private TCPProducerBroker udpProducerBroker;
    private QueueConsumer queueConsumer;
    private TCPConsumerBroker udpConsumerBroker;
    private QueueProducer queueProducer;


    public static void main(String[] args) {
        JLBHOptions jlbhOptions = new JLBHOptions()
                .warmUpIterations(10_000).iterations(5_000_000).throughput(1_000_000).runs(3)
                .accountForCoordinatedOmission(false).recordOSJitter(false).jlbhTask(new JLBHTCPProducerBroker());

        new JLBH(jlbhOptions).start();
    }

    @Override
    public void init(JLBH jlbh) {
        this.jlbh = jlbh;
        try {
            udpProducerBroker = new TCPProducerBroker();
            udpProducerBroker.init();
            udpConsumerBroker = new TCPConsumerBroker();
            queueConsumer = new QueueConsumer();
            queueProducer = new QueueProducer();
            initialize();
        } catch (IOException exp) {
            LOG.error("Failed to initialize JLBH", exp);
        }
    }

    @Override
    public void run(long startTimeNS) {
        udpProducerBroker.sendNext();
        jlbh.sampleNanos((nanoTime() - 10) - startTimeNS);
    }


    private void initialize() {
        new Thread(() -> queueProducer.produce()).start();
        new Thread(() -> queueConsumer.consume()).start();
        new Thread(() -> udpConsumerBroker.startBroker()).start();
    }

}