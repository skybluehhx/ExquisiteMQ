package com.lin.client.consumer;

import com.lin.commons.exception.UnknowCodecTypeException;
import com.lin.commons.utils.codec.SerializerFactory;
import com.lin.commons.utils.codec.SerializerObject;

/**
 * @author jianglinzou
 * @date 2019/3/15 下午5:12
 */
public abstract class AbstractRecoverManager implements RecoverManager {
    private final String META_RECOVER_CODEC_TYPE = System.getProperty("meta.recover.codec", "java");
    protected final SerializerObject serializer;


    public AbstractRecoverManager() {
        if (this.META_RECOVER_CODEC_TYPE.equals("java")) {
            this.serializer = SerializerFactory.buildSerializer(SerializerFactory.Codec_Type.JAVA);

        } else if (this.META_RECOVER_CODEC_TYPE.equals("hessian")) {
            this.serializer = SerializerFactory.buildSerializer(SerializerFactory.Codec_Type.HESSIAN);
        } else {
            throw new UnknowCodecTypeException(this.META_RECOVER_CODEC_TYPE);
        }
    }

}
