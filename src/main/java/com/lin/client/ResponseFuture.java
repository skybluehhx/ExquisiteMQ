package com.lin.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author jianglinzou
 * @date 2019/3/13 下午10:24
 */
public class ResponseFuture<T, V> implements Future<T> {

    // 因为请求和响应是一一对应的，因此初始化CountDownLatch值为1。
    private CountDownLatch latch = new CountDownLatch(1);
    // 需要响应线程设置的响应结果
    private volatile T response;

    private V request;

    // Futrue的请求时间，用于计算Future是否超时
    private long beginTime = System.currentTimeMillis();

    public ResponseFuture() {
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        if (response != null) {
            return true;
        }
        return false;
    }

    // 获取响应结果，直到有结果才返回。
    @Override
    public T get() throws InterruptedException {
        latch.await();
        return this.response;
    }

    // 获取响应结果，直到有结果或者超过指定时间就返回。
    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException {
        if (latch.await(timeout, unit)) {
            return this.response;
        }
        throw new RuntimeException("超时获取结果");
    }

    // 用于设置响应结果，并且做countDown操作，通知请求线程
    public void setResponse(T response) {
        this.response = response;
        latch.countDown();
    }

    public long getBeginTime() {
        return beginTime;
    }


    public void setRequest(V request) {
        this.request = request;
    }

}
