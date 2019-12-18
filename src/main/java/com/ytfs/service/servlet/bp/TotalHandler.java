package com.ytfs.service.servlet.bp;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.BlockAccessor;
import com.ytfs.service.dao.UserAccessor;
import com.ytfs.service.packet.bp.TotalReq;
import com.ytfs.service.packet.bp.TotalResp;
import com.ytfs.service.servlet.Handler;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import org.apache.log4j.Logger;
import org.bson.Document;

public class TotalHandler extends Handler<TotalReq> {

    private static final Logger LOG = Logger.getLogger(TotalHandler.class);

    @Override
    public Object handle() throws Throwable {
        try {
            getSuperNodeId();
        } catch (NodeMgmtException e) {
            LOG.error("Invalid super node pubkey:" + this.getPublicKey());
            return new ServiceException(ServiceErrorCode.INVALID_NODE_ID, e.getMessage());
        }
        return getTotal();
    }

    private static TotalResp getTotal() throws Throwable {
        TotalResp resp = new TotalResp();
        Document doc = UserAccessor.total();
        resp.setBkTotal(doc.getLong("blockTotal"));
        resp.setFileTotal(doc.getLong("fileTotal"));
        resp.setSpaceTotal(doc.getLong("spaceTotal"));
        resp.setUsedspace(doc.getLong("usedspace"));
        resp.setActualBkTotal(BlockAccessor.getBlockCount());
        return resp;
    }
}
