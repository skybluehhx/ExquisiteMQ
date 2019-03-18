package com.lin.client.consumer;

import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 稳定排序的delay queue，线程安全
 * @author jianglinzou
 * @date 2019/3/11 下午1:25
 */
public class FetchRequestQueue {

    private final LinkedList<FetchRequest> queue = new LinkedList<FetchRequest>();
    private final Lock lock = new ReentrantLock();
    private final Condition available = this.lock.newCondition();

    /**
     * Thread designated to wait for the element at the head of the queue. This
     * variant of the Leader-Follower pattern
     * (http://www.cs.wustl.edu/~schmidt/POSA/POSA2/) serves to minimize
     * unnecessary timed waiting. When a thread becomes the leader, it waits
     * only for the next delay to elapse, but other threads await indefinitely.
     * The leader thread must signal some other thread before returning from
     * take() or poll(...), unless some other thread becomes leader in the
     * interim. Whenever the head of the queue is replaced with an element with
     * an earlier expiration time, the leader field is invalidated by being
     * reset to null, and some waiting thread, but not necessarily the current
     * leader, is signalled. So waiting threads must be prepared to acquire and
     * lose leadership while waiting.
     */
    private Thread leader = null;


    public FetchRequest take() throws InterruptedException {
        final Lock lock = this.lock;
        lock.lockInterruptibly();
        try {
            for (;;) {
                FetchRequest first = this.queue.peek();
                if (first == null) {
                    this.available.await();
                }
                else {
                    long delay = first.getDelay(TimeUnit.NANOSECONDS);
                    if (delay <= 0) {
                        return this.queue.poll();
                    }
                    else if (this.leader != null) {
                        this.available.await();
                    }
                    else {
                        Thread thisThread = Thread.currentThread();
                        this.leader = thisThread;
                        try {
                            this.available.awaitNanos(delay);
                        }
                        finally {
                            if (this.leader == thisThread) {
                                this.leader = null;
                            }
                        }
                    }
                }
            }
        }
        finally {
            if (this.leader == null && this.queue.peek() != null) {
                this.available.signal();
            }
            lock.unlock();
        }
    }


    public void offer(FetchRequest e) {
        final Lock lock = this.lock;
        lock.lock();
        try {
            /**
             * A request is not referenced by this queue,so we don't want to add
             * it.
             */
            if (e.getRefQueue() != null && e.getRefQueue() != this) {
                return;
            }
            // Reference to request.
            e.setRefQueue(this);
            this.queue.offer(e);
            Collections.sort(this.queue);
            // Leader is changed.
            if (this.queue.peek() == e) {
                this.leader = null;
                this.available.signal();
            }
        }
        finally {
            lock.unlock();
        }
    }


    public int size() {
        final Lock lock = this.lock;
        lock.lock();
        try {
            return this.queue.size();
        }
        finally {
            lock.unlock();
        }
    }
}
