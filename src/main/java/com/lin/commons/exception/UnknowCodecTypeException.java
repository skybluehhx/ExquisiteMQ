package com.lin.commons.exception;

/**
 * @author jianglinzou
 * @date 2019/3/15 下午5:16
 */
public class UnknowCodecTypeException extends RuntimeException {


    /**
     *
     */
    private static final long serialVersionUID = 1L;


    public UnknowCodecTypeException() {
        super();

    }


    public UnknowCodecTypeException(String message, Throwable cause) {
        super(message, cause);

    }


    public UnknowCodecTypeException(String message) {
        super(message);

    }


    public UnknowCodecTypeException(Throwable cause) {
        super(cause);

    }
}
