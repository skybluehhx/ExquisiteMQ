package com.lin.client.netty;

import com.lin.commons.utils.network.Response;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 网络回调监听器，用于异步发送，
 * 接受响应成功后将会触发
 *
 * @author jianglinzou
 * @date 2019/3/18 下午2:39
 */
public interface SingleRequestCallBackListener {

    void onResponse(Response response);


    void onException(Exception exception);

    Executor getExecutor();

}
