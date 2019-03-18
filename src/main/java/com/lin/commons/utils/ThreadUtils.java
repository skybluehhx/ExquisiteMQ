package com.lin.commons.utils;

/**
 * @author jianglinzou
 * @date 2019/3/11 下午1:13
 */
public class ThreadUtils {

    public static RuntimeException launderThrowable(Throwable t) {
        if (t instanceof RuntimeException) {
            return (RuntimeException) t;
        } else if (t instanceof Error) {
            throw (Error) t;
        } else {
            throw new IllegalStateException("Not unchecked", t);
        }
    }

}
