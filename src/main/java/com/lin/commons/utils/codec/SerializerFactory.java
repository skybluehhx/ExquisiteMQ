package com.lin.commons.utils.codec;

import com.lin.commons.utils.codec.impl.HessianSerializerObject;
import com.lin.commons.utils.codec.impl.JavaSerializerObject;

import java.util.HashMap;
import java.util.Map;

/**
 * @author jianglinzou
 * @date 2019/3/8 下午6:34
 */
public class SerializerFactory {

    public static final Map<Codec_Type, SerializerObject> serializerMap = new HashMap<Codec_Type, SerializerObject>() {{
        put(Codec_Type.JAVA, new JavaSerializerObject());
        put(Codec_Type.HESSIAN, new HessianSerializerObject());
    }};


    public static SerializerObject buildSerializer(final Codec_Type type) {
        return serializerMap.get(type);
    }


    //序列化类型
    public static enum Codec_Type {
        JAVA,
        HESSIAN;

        public static Codec_Type parseByte(final byte type) {
            switch (type) {
                case 0:
                    return JAVA;
                case 1:
                    return HESSIAN;
            }
            throw new IllegalArgumentException("Invalid Codec type: " + "现在只支持JAVA, HESSIAN及其SIMPLE.");
        }


        public byte toByte() {
            switch (this) {
                case JAVA:
                    return 0;
                case HESSIAN:
                    return 1;
            }
            throw new IllegalArgumentException("Invalid Codec type: " + "现在只支持JAVA, HESSIAN及其SIMPLE.");
        }
    }

}
