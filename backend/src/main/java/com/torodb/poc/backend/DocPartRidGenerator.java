package com.torodb.poc.backend;

import java.util.concurrent.atomic.AtomicInteger;

public class DocPartRidGenerator {
    private final AtomicInteger lastRid = new AtomicInteger(0);
    
    //TODO: Move and refactor
    public int nextRid() {
        return lastRid.getAndIncrement();
    }
}
