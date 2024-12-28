package com.abidi.config;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class Config {

    public final String targetIP;
    private final int targetPort;
    private final String sourceIP;
    private final int sourcePort;
    private final int queueSize;
    private final String queuePath;
    private final String queueName;

    public void init(String configFile ) {


    }
}
