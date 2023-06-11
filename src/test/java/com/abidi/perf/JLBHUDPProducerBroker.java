package com.abidi.perf;

import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JLBHUDPProducerBroker implements JLBHTask {

    private static final Logger LOG = LoggerFactory.getLogger(JLBHUDPProducerBroker.class);


    private JLBH jlbh;

    @Override
    public void init(JLBH jlbh) {

    }

    @Override
    public void run(long startTimeNS) {

    }
}
