package com.lin.commons.exception;

/**
 * 客户端异常基类
 * @author jianglinzou
 * @date 2019/3/8 下午6:19
 */
public class SimpleMQClientException extends  Exception {

    static final long serialVersionUID = -1L;


    public SimpleMQClientException() {
        super();

    }


    public SimpleMQClientException(String message, Throwable cause) {
        super(message, cause);

    }


    public SimpleMQClientException(String message) {
        super(message);

    }


    public SimpleMQClientException(Throwable cause) {
        super(cause);

    }

}
