package com.lin.commons.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author jianglinzou
 * @date 2019/3/9 下午9:43
 */
public class DefaultExceptionMonitor extends ExceptionMonitor {
    private final Log log = LogFactory.getLog(DefaultExceptionMonitor.class);

    public DefaultExceptionMonitor() {
    }

    public void exceptionCaught(Throwable cause) {
        if (this.log.isErrorEnabled()) {
            this.log.error("Gecko unexpected exception", cause);
        }

    }
}
