package com.lin.commons.utils;

/**
 * @author jianglinzou
 * @date 2019/3/9 下午9:42
 */
public abstract class ExceptionMonitor {

    private static ExceptionMonitor instance = new DefaultExceptionMonitor();

    public ExceptionMonitor() {
    }

    public static ExceptionMonitor getInstance() {
        return instance;
    }

    public static void setInstance(ExceptionMonitor monitor) {
        if (monitor == null) {
            monitor = new DefaultExceptionMonitor();
        }

        instance = (ExceptionMonitor)monitor;
    }

    public abstract void exceptionCaught(Throwable var1);
}
