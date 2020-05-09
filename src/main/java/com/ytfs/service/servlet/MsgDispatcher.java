package com.ytfs.service.servlet;

import com.ytfs.common.SerializationUtil;
import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.p2phost.interfaces.Callback;
import org.apache.log4j.Logger;

public class MsgDispatcher implements Callback {

    private static final Logger LOG = Logger.getLogger(MsgDispatcher.class);

    @Override
    public byte[] onMessage(byte[] data, String nodekey) {
        Object message;
        try {
            message = SerializationUtil.deserialize(data);
        } catch (Throwable r) {
            LOG.error("Deserialize ERR:", r);
            return SerializationUtil.serialize(new ServiceException(ServiceErrorCode.SERVER_ERROR));
        }
        try {
            Handler handler = HandlerFactory.getHandler(message);
            handler.setPubkey(nodekey);
            Object response = handler.handle();
            return SerializationUtil.serialize(response);
        } catch (ServiceException s) {
            LOG.error("", s);
            return SerializationUtil.serialize(s);
        } catch (NodeMgmtException ne) {
            LOG.error("", ne);
            ServiceException se = new ServiceException(ServiceErrorCode.INVALID_NODE_ID, ne.getMessage());
            return SerializationUtil.serialize(se);
        } catch (Throwable r) {
            LOG.error("", r);
            ServiceException se = new ServiceException(ServiceErrorCode.SERVER_ERROR, r.getMessage());
            return SerializationUtil.serialize(se);
        }
    }
}
