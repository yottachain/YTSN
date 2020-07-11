package com.ytfs.service.servlet;

import com.ytfs.common.SerializationUtil;
import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.p2phost.interfaces.Callback;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;

public class MsgDispatcher implements Callback {

    private static final Logger LOG = Logger.getLogger(MsgDispatcher.class);
    private static final AtomicInteger readnum=new AtomicInteger(0);
    private static final AtomicInteger writenum=new AtomicInteger(0);
    private static final AtomicInteger syncnum=new AtomicInteger(0);

    @Override
    public byte[] onMessage(byte[] data, String nodekey) {
        Object message;
        try {
            message = SerializationUtil.deserialize(data);
        } catch (Throwable r) {
            LOG.error("Deserialize ERR:", r);
            return SerializationUtil.serialize(new ServiceException(ServiceErrorCode.SERVER_ERROR));
        }
        AtomicInteger num=null;
        int limit=1000;
        try {
            Handler handler = HandlerFactory.getHandler(message);
            handler.setPubkey(nodekey);
            if (handler.GetDoType()==1){
                num=writenum;
                limit=2000;
            }else if (handler.GetDoType()==2){
                num=syncnum;
                limit=2000;
            }else{
                num=readnum;
                limit=500;
            }
            if (num.getAndIncrement()>=limit){
                ServiceException se = new ServiceException(ServiceErrorCode.SERVER_ERROR);
                return SerializationUtil.serialize(se);
            }
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
        }finally{
            num.decrementAndGet();
        }
    }
}
