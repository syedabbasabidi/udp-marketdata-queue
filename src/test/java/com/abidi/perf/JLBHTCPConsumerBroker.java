package com.abidi.perf;

import com.abidi.broker.TCPConsumerBroker;
import com.abidi.broker.TCPProducerBroker;
import com.abidi.broker.UDPConsumerBroker;
import com.abidi.broker.UDPProducerBroker;
import com.abidi.consumer.UDPQueueConsumer;
import com.abidi.producer.UDPQueueProducer;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static java.lang.System.nanoTime;

public class JLBHTCPConsumerBroker implements JLBHTask {

    private static final Logger LOG = LoggerFactory.getLogger(JLBHTCPConsumerBroker.class);

    private JLBH jlbh;
    private TCPProducerBroker tcpProducerBroker;
    private UDPQueueConsumer udpQueueConsumer;
    private TCPConsumerBroker tcpConsumerBroker;
    private UDPQueueProducer udpQueueProducer;


    public static void main(String[] args) {
        JLBHOptions jlbhOptions = new JLBHOptions()
                .warmUpIterations(10_000).iterations(5_000_000).throughput(1_000_000).runs(3)
                .accountForCoordinatedOmission(false).recordOSJitter(false).jlbhTask(new JLBHTCPConsumerBroker());

        new JLBH(jlbhOptions).start();
    }

    @Override
    public void init(JLBH jlbh) {
        this.jlbh = jlbh;
        try {
            tcpConsumerBroker = new TCPConsumerBroker();
            tcpProducerBroker = new TCPProducerBroker();
            udpQueueConsumer = new UDPQueueConsumer();
            udpQueueProducer = new UDPQueueProducer();
            initialize();
        } catch (IOException exp) {
            LOG.error("Failed to initialize JLBH", exp);
        }
    }

    @Override
    public void run(long startTimeNS) {
        tcpConsumerBroker.getNextAndAck();
        jlbh.sampleNanos((nanoTime() - 10) - startTimeNS);
    }


    private void initialize() {
        new Thread(() -> udpQueueProducer.produce()).start();
        new Thread(() -> udpQueueConsumer.consume()).start();
        new Thread(() -> tcpProducerBroker.startBroker()).start();
    }

}