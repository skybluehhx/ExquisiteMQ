package com.lin.commons.utils.network;

import lombok.Data;
import lombok.ToString;

/**
 * 统一传输对象
 *
 * @author jianglinzou
 * @date 2019/3/13 上午11:13
 */
@Data
@ToString
public class Transportation {

    private Header header;


    private String body;//请求体


}
