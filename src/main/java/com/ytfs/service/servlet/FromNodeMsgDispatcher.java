package com.ytfs.service.servlet;

import com.ytfs.service.utils.SerializationUtil;
import com.ytfs.service.utils.ServiceErrorCode;
import com.ytfs.service.utils.ServiceException;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.p2phost.interfaces.NodeCallback;
import org.apache.log4j.Logger;

public class FromNodeMsgDispatcher implements NodeCallback {

    private static final Logger LOG = Logger.getLogger(FromNodeMsgDispatcher.class);

    @Override
    public byte[] onMessageFromNode(byte[] data, String nodekey) {
        try {
            Object message = SerializationUtil.deserialize(data);
            Handler handler = HandlerFactory.getHandler(message);
            handler.setRequest(message);
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
