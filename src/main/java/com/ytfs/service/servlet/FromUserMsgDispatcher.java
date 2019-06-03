package com.ytfs.service.servlet;

import com.ytfs.common.SerializationUtil;
import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import io.yottachain.p2phost.interfaces.UserCallback;
import org.apache.log4j.Logger;

public class FromUserMsgDispatcher implements UserCallback {

    private static final Logger LOG = Logger.getLogger(FromUserMsgDispatcher.class);

    @Override
    public byte[] onMessageFromUser(byte[] data, String userkey) {
        try {
            Object message = SerializationUtil.deserialize(data);
            Handler handler = HandlerFactory.getHandler(message);
            handler.setRequest(message);
            handler.setPubkey(userkey);
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
