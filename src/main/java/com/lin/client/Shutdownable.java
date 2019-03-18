package com.lin.client;

import com.lin.commons.exception.SimpleMQClientException;

/**
 * @author jianglinzou
 * @date 2019/3/11 下午1:16
 */
public interface Shutdownable {

    public void shutdown() throws SimpleMQClientException;
}
