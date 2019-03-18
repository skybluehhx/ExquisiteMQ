package com.lin.commons.exception;

/**
 * @author jianglinzou
 * @date 2019/3/9 下午10:13
 */
public class NotifyRemotingException  extends Exception{

    static final long serialVersionUID = 8923187437857838L;

    public NotifyRemotingException() {
    }

    public NotifyRemotingException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotifyRemotingException(String message) {
        super(message);
    }

    public NotifyRemotingException(Throwable cause) {
        super(cause);
    }
}
