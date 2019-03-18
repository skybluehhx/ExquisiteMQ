package com.lin.commons.utils.network;

import lombok.Data;
import lombok.ToString;

/**
 * 响应
 *
 * @author jianglinzou
 * @date 2019/3/13 上午10:49
 */
@Data
@ToString
public class Response extends Transportation {


    private Request request;

}
