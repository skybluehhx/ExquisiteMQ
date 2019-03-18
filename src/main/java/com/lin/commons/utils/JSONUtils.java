package com.lin.commons.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

/**
 * @author jianglinzou
 * @date 2019/3/11 下午1:11
 */
public class JSONUtils {

    private static final Logger logger = LoggerFactory.getLogger(JSONUtils.class);


    public static String serializeObject(final Object o) throws Exception {
        return mapper.writeValueAsString(o);
    }

    static ObjectMapper mapper = new ObjectMapper();


    public static Object deserializeObject(final String s, final Class<?> clazz) throws Exception {
        return mapper.readValue(s, clazz);
    }


    public static Object deserializeObject(final String s, final TypeReference<?> typeReference) throws Exception {
        return mapper.readValue(s, typeReference);
    }


    public final static String EMPTY_MAP_JSON = "{}";
    public final static String EMPTY_LIST_JSON = "[]";


    public static String toJsonString(Object obj) {
        if (obj == null) {
            return EMPTY_MAP_JSON;
        }
        if (obj instanceof String) {
            return (String) obj;
        }
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (IOException e) {
            logger.error("to json string error:{}.", obj, e);
            return StringUtils.EMPTY;
        }
    }

    public static <E> JSONArray collections2JsonArray(Collection<E> c) {
        JSONArray ret = new JSONArray();
        ret.addAll(c);
        return ret;
    }

    public static <T> T getObject(String json, Class<T> objClass) {
        try {
            return JSON.parseObject(json, objClass);
        } catch (Exception e) {
            logger.warn("parse json error:{}", json, e);
        }
        return null;
    }


    public static JSONObject getJSONObject(String jsonStr) {
        if (jsonStr == null) {
            return null;
        }
        try {
            JSONObject account = JSONObject.parseObject(jsonStr);
            return account;
        } catch (Exception e) {
            logger.info(" parse json  error, jsonStr is {}", jsonStr);
            return null;
        }
    }

    @SuppressWarnings("Duplicates")
    public static <T> Optional<T> parseJson(String json, Class<T> objClass) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            return Optional.ofNullable(objectMapper.readValue(json, objClass));
        } catch (IOException e) {
            logger.warn("parse json error:{}.", json, e);
            return Optional.empty();
        }
    }

    @SuppressWarnings("Duplicates")
    public static <T> Optional<T> parseJson(String json, TypeReference<T> objClass) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            return Optional.ofNullable(objectMapper.readValue(json, objClass));
        } catch (IOException e) {
            logger.warn("parse json error:{}.", json, e);
            return Optional.empty();
        }
    }


}





