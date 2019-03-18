package com.lin.commons.utils;

/**
 * @author jianglinzou
 * @date 2019/3/9 下午9:29
 */
public class LongSequenceGenerator {

    private static long lastSequenceId;


    public static synchronized long getNextSequenceId() {
        return ++lastSequenceId;
    }


    public static synchronized long getLastSequenceId() {
        return lastSequenceId;
    }


    public static synchronized void setLastSequenceId(final long l) {
        lastSequenceId = l;
    }


}
