package com.lin.server.Exception;

import lombok.Data;

/**
 * @author jianglinzou
 * @date 2019/3/14 下午2:48
 */
@Data
public class FailLoadConfFileException extends ExquisiteMQException {
    private String msg;

    public FailLoadConfFileException(Throwable e) {
        super(e);
        this.msg = e.getMessage();
    }

    public FailLoadConfFileException(String msg) {
        super(msg);
        this.msg = msg;
    }


}
