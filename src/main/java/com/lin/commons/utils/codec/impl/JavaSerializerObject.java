package com.lin.commons.utils.codec.impl;

import com.lin.commons.utils.codec.SerializerObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;

/**
 * @author jianglinzou
 * @date 2019/3/8 下午6:25
 */
public class JavaSerializerObject implements SerializerObject {

    private final Log logger = LogFactory.getLog(JavaSerializerObject.class);

    /**
     * 反序列化对象
     *
     * @param objContent - 指定的字节码内容
     * @return
     * @throws IOException
     */
    public Object decodeObject(byte[] objContent) throws IOException {
        Object obj = null;
        ByteArrayInputStream bais = null;
        ObjectInputStream ois = null;
        try {
            bais = new ByteArrayInputStream(objContent);
            ois = new ObjectInputStream(bais);
            obj = ois.readObject();
        } catch (final IOException ex) {
            throw ex;
        } catch (final ClassNotFoundException ex) {
            this.logger.warn("Failed to decode object.", ex);
        } finally {
            if (ois != null) {
                try {
                    ois.close();
                    bais.close();
                } catch (final IOException ex) {
                    this.logger.error("Failed to close stream.", ex);
                }
            }
        }
        return obj;
    }


    /**
     * 序列化对象
     *
     * @param objContent
     * @return
     * @throws IOException
     */
    public byte[] encodeObject(Object objContent) throws IOException {
        ByteArrayOutputStream baos = null;
        ObjectOutputStream output = null;
        try {
            baos = new ByteArrayOutputStream(1024);
            output = new ObjectOutputStream(baos);
            output.writeObject(objContent);
        } catch (final IOException ex) {
            throw ex;

        } finally {
            if (output != null) {
                try {
                    output.close();
                    if (baos != null) {
                        baos.close();
                    }
                } catch (final IOException ex) {
                    this.logger.error("Failed to close stream.", ex);
                }
            }
        }
        return baos != null ? baos.toByteArray() : null;
    }
}
