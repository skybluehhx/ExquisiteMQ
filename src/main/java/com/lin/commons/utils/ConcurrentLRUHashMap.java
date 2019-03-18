package com.lin.commons.utils;

import java.util.concurrent.locks.ReentrantLock;

/**
 * @author jianglinzou
 * @date 2019/3/15 下午6:00
 */
public class ConcurrentLRUHashMap {
    private final LRUHashMap<String, Byte> innerMap;
    private final ReentrantLock lock;


    public ConcurrentLRUHashMap() {
        this(1024);
    }


    public int size() {
        this.lock.lock();
        try {
            return this.innerMap.size();
        } finally {
            this.lock.unlock();
        }
    }


    public ConcurrentLRUHashMap(int capacity) {
        this.innerMap = new LRUHashMap<String, Byte>(capacity, true);
        this.lock = new ReentrantLock();
    }


    public void put(String k, Byte v) {
        this.lock.lock();
        try {
            this.innerMap.put(k, v);
        } finally {
            this.lock.unlock();
        }
    }


    public Byte get(String k) {
        this.lock.lock();
        try {
            return this.innerMap.get(k);
        } finally {
            this.lock.unlock();
        }
    }

}
