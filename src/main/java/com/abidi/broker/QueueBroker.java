package com.abidi.broker;

import com.abidi.queue.CircularMMFQueue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class QueueBroker {

    private final ExecutorService executorService =  Executors.newSingleThreadExecutor(new BrokerThreadFactory());
    private final CircularMMFQueue circularMMFQueue;

    public QueueBroker(CircularMMFQueue circularMMFQueue) {
        this.circularMMFQueue = circularMMFQueue;
    }

    public void start() {
        executorService.submit(() -> process());
    }

    private void process() {
        while(true) {
            byte[] bytes = circularMMFQueue.get();
            if(bytes != null) sendItAcross(bytes);
        }
    }

    private void sendItAcross(byte[] bytes) {
    }

    static class BrokerThreadFactory implements ThreadFactory {
        public Thread newThread(Runnable r) {
            return new Thread(r, "QueueBroker");
        }
    }
}
