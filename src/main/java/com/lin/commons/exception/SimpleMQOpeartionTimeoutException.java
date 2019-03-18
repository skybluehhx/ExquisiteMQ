package com.lin.commons.exception;

/**
 * @author jianglinzou
 * @date 2019/3/9 下午10:07
 */
public class SimpleMQOpeartionTimeoutException extends SimpleMQClientException {


    static final long serialVersionUID = -1L;


    public SimpleMQOpeartionTimeoutException() {
        super();

    }


    public SimpleMQOpeartionTimeoutException(String message, Throwable cause) {
        super(message, cause);

    }


    public SimpleMQOpeartionTimeoutException(String message) {
        super(message);

    }


    public SimpleMQOpeartionTimeoutException(Throwable cause) {
        super(cause);

    }

}
