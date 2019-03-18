package com.lin.server.Exception;

/**
 * @author jianglinzou
 * @date 2019/3/14 下午3:18
 */
public class ExquisiteMQException extends Exception {

    public ExquisiteMQException(Throwable e) {
        super(e);
    }

    public ExquisiteMQException(String msg) {

        super(msg);
    }
}
