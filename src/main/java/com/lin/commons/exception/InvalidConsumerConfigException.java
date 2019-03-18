package com.lin.commons.exception;

/**
 * @author jianglinzou
 * @date 2019/3/9 下午9:55
 */
public class InvalidConsumerConfigException extends IllegalArgumentException {


    private static final long serialVersionUID = 1L;


    public InvalidConsumerConfigException() {
        super();

    }


    public InvalidConsumerConfigException(String message, Throwable cause) {
        super(message, cause);

    }


    public InvalidConsumerConfigException(String s) {
        super(s);

    }


    public InvalidConsumerConfigException(Throwable cause) {
        super(cause);

    }

}
