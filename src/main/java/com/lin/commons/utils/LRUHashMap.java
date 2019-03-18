package com.lin.commons.utils;

import java.util.LinkedHashMap;

/**
 * @author jianglinzou
 * @date 2019/3/15 下午6:01
 */
public class LRUHashMap<K,V> extends LinkedHashMap<K,V> {


    private final int maxCapacity;

    static final long serialVersionUID = 438971390573954L;

    private static final float DEFAULT_LOAD_FACTOR = 0.75f;

    private transient EldestEntryHandler<K, V> handler;

    private boolean enableLRU;


    public void setHandler(final EldestEntryHandler<K, V> handler) {
        this.handler = handler;
    }

    public interface EldestEntryHandler<K, V> {
        public boolean process(java.util.Map.Entry<K, V> eldest);
    }


    public LRUHashMap() {
        this(1000, true);
    }


    public LRUHashMap(final int maxCapacity, final boolean enableLRU) {
        super(maxCapacity, DEFAULT_LOAD_FACTOR, true);
        this.maxCapacity = maxCapacity;
        this.enableLRU = enableLRU;
    }


    @Override
    protected boolean removeEldestEntry(final java.util.Map.Entry<K, V> eldest) {
        if (!this.enableLRU) {
            return false;
        }
        final boolean result = this.size() > maxCapacity;
        if (result && handler != null) {
            // 成功存入磁盘，即从内存移除，否则继续保留在保存
            return handler.process(eldest);
        }
        return result;
    }
}
