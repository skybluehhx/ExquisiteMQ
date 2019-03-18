package com.lin.commons.exception;

/**
 * @author jianglinzou
 * @date 2019/3/15 下午5:25
 */
public class InvalidOffsetStorageException extends IllegalArgumentException  {
    /**
     *
     */
    private static final long serialVersionUID = 1L;


    public InvalidOffsetStorageException() {
        super();

    }


    public InvalidOffsetStorageException(String message, Throwable cause) {
        super(message, cause);

    }


    public InvalidOffsetStorageException(String s) {
        super(s);

    }


    public InvalidOffsetStorageException(Throwable cause) {
        super(cause);

    }

}
