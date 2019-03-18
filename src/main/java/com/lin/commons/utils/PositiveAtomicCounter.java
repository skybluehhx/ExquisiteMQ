package com.lin.commons.utils;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jianglinzou
 * @date 2019/3/9 下午9:19
 */
public class PositiveAtomicCounter {

    private final AtomicInteger atom = new AtomicInteger(0);
    private static final int mask = 2147483647;

    public PositiveAtomicCounter() {
    }

    public final int incrementAndGet() {
        int rt = this.atom.incrementAndGet();
        return rt & 2147483647;
    }

    public int intValue() {
        return this.atom.intValue();
    }

}
