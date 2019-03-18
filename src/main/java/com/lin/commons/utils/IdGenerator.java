package com.lin.commons.utils;

import com.lin.commons.utils.network.RemotingUtils;

import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * id生成器，生成全局唯一strings,可为生成者，生成唯一id
 *  Generator for Globally unique Strings
 * @author jianglinzou
 * @date 2019/3/11 下午1:11
 */
public class IdGenerator {
    private String seed;
    private final AtomicLong sequence = new AtomicLong(1);
    private int length;


    /**
     * Construct an IdGenerator
     */
    public IdGenerator() {
        try {
            this.seed = RemotingUtils.getLocalHost() + "-" + System.currentTimeMillis() + "-";
            this.length = this.seed.length() + ("" + Long.MAX_VALUE).length();
        }
        catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Generate a unqiue id
     *
     * @return a unique id
     */

    public  synchronized String generateId() {
        final StringBuilder sb = new StringBuilder(this.length);
        sb.append(this.seed);
        sb.append(this.sequence.getAndIncrement());
        return sb.toString();
    }


    /**
     * Generate a unique ID - that is friendly for a URL or file system
     *
     * @return a unique id
     */
    public String generateSanitizedId() {
        String result = this.generateId();
        result = result.replace(':', '-');
        result = result.replace('_', '-');
        result = result.replace('.', '-');
        return result;
    }

}
