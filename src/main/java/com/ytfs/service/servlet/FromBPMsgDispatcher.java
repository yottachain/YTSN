package com.ytfs.service.servlet;

import com.ytfs.common.SerializationUtil;
import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import io.yottachain.p2phost.interfaces.BPNodeCallback;
import org.apache.log4j.Logger;

public class FromBPMsgDispatcher implements BPNodeCallback {

    private static final Logger LOG = Logger.getLogger(FromBPMsgDispatcher.class);

    @Override
    public byte[] onMessageFromBPNode(byte[] bytes, String string) {
        Object message;
        try {
            message = SerializationUtil.deserialize(bytes);
        } catch (Throwable r) {
            LOG.error("Deserialize ERR:" + r);
            return SerializationUtil.serialize(new ServiceException(ServiceErrorCode.SERVER_ERROR));
        }
        try {
            Handler handler = HandlerFactory.getHandler(message);
            handler.setPubkey(string);
            Object response = handler.handle();
            return SerializationUtil.serialize(response);
        } catch (ServiceException s) {
            LOG.error("", s);
            return SerializationUtil.serialize(s);
        } catch (Throwable r) {
            LOG.error("", r);
            ServiceException se = new ServiceException(ServiceErrorCode.SERVER_ERROR, r.getMessage());
            return SerializationUtil.serialize(se);
        }
    }

}
