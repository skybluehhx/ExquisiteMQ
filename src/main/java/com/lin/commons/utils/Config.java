package com.lin.commons.utils;

import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

/**
 * @author jianglinzou
 * @date 2019/3/11 下午1:08
 */
public class Config {

    /**
     * 获取配置类的字段
     * 如果标有@Ignore则该字段会被忽略
     * 如果标有@Key注解，则会使用该注解中的内容代替
     * @return
     */
    public Set<String> getFieldSet() {
        Class<? extends Config> clazz = this.getClass();
        Field[] fields = clazz.getDeclaredFields();
        Set<String> rt = new HashSet<String>();
        for (Field f : fields) {
            String name = f.getName();
            Ignore ignore = f.getAnnotation(Ignore.class);
            if (ignore != null) {
                continue;
            }
            Key key = f.getAnnotation(Key.class);
            if (key != null) {
                name = key.name();
                if (!StringUtils.isBlank(name)) {
                    rt.add(name);
                }
                else {
                    rt.add(f.getName());
                }
            }
            else if (name.length() > 0 && Character.isLowerCase(name.charAt(0))) {
                rt.add(name);
            }
        }
        return rt;
    }

    /**
     * 遍历fields，找出其中与value最匹配的值
     * @param fields
     * @param value
     * @return
     */
    public String findBestMatchField(Set<String> fields, String value) {
        int minScore = Integer.MAX_VALUE; //记录匹配度
        String matchedField = null; //记录最匹配的value
        for (String f : fields) {
            int dis = StringUtils.getLevenshteinDistance(value, f);
            if (dis < minScore) {
                matchedField = f;
                minScore = dis;
            }
        }
        return matchedField;
    }


    public void checkConfigKeys(Set<String> configKeySet, Set<String> validKeySet) {
        for (String key : configKeySet) {
            if (!validKeySet.contains(key)) {
                String best = this.findBestMatchField(validKeySet, key);
                throw new IllegalArgumentException("Invalid config key:" + key + ",do you mean '" + best + "'?");
            }
        }
    }
}
