package com.lin.commons.utils.codec.impl;

import com.caucho.hessian.io.HessianInput;
import com.caucho.hessian.io.HessianOutput;
import com.lin.commons.utils.codec.SerializerObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author jianglinzou
 * @date 2019/3/8 下午6:26
 */
public class HessianSerializerObject<T> implements SerializerObject<T> {


    private final Log logger = LogFactory.getLog(HessianSerializerObject.class);

    public T decodeObject(byte[] in) throws IOException {
        Object obj = null;
        ByteArrayInputStream bais = null;
        HessianInput input = null;
        try {
            bais = new ByteArrayInputStream(in);
            input = new HessianInput(bais);
            input.startReply();
            obj = input.readObject();
            input.completeReply();
        } catch (final IOException ex) {
            throw ex;
        } catch (final Throwable e) {
            this.logger.error("Failed to decode object.", e);
        } finally {
            if (input != null) {
                try {
                    bais.close();
                } catch (final IOException ex) {
                    this.logger.error("Failed to close stream.", ex);
                }
            }
        }
        return (T)obj;
    }

    public byte[] encodeObject(T obj) throws IOException {
        ByteArrayOutputStream baos = null;
        HessianOutput output = null;
        try {
            baos = new ByteArrayOutputStream(1024);
            output = new HessianOutput(baos);
            output.startCall();
            output.writeObject(obj);
            output.completeCall();
        } catch (final IOException ex) {
            throw ex;
        } finally {
            if (output != null) {
                try {
                    baos.close();
                } catch (final IOException ex) {
                    this.logger.error("Failed to close stream.", ex);
                }
            }
        }
        return baos != null ? baos.toByteArray() : null;
    }
}

