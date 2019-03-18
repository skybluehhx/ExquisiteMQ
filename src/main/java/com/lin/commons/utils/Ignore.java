package com.lin.commons.utils;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author jianglinzou
 * @date 2019/3/11 下午1:08
 */

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD })
public @interface Ignore {
}
