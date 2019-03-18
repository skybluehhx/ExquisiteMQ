package com.lin.commons.exception;

/**
 * @author jianglinzou
 * @date 2019/3/9 下午10:11
 */
public class InvalidMessageException extends SimpleMQClientException {

    static final long serialVersionUID = -1L;


    public InvalidMessageException() {
        super();

    }


    public InvalidMessageException(String message, Throwable cause) {
        super(message, cause);

    }


    public InvalidMessageException(String message) {
        super(message);

    }


    public InvalidMessageException(Throwable cause) {
        super(cause);

    }

}
