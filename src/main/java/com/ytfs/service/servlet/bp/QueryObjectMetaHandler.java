package com.ytfs.service.servlet.bp;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.servlet.Handler;
import com.ytfs.common.ServiceException;
import com.ytfs.service.packet.ObjectRefer;
import com.ytfs.service.packet.bp.QueryObjectMetaReq;
import com.ytfs.service.packet.bp.QueryObjectMetaResp;
import com.ytfs.service.servlet.ReferCache;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.List;
import org.apache.log4j.Logger;

public class QueryObjectMetaHandler extends Handler<QueryObjectMetaReq> {

    private static final Logger LOG = Logger.getLogger(QueryObjectMetaHandler.class);

    public static QueryObjectMetaResp queryObjectMetaCall(QueryObjectMetaReq req) throws ServiceException {
        QueryObjectMetaResp resp;
        SuperNode node = SuperNodeList.getUserSuperNode(req.getUserID());
        if (node.getId() == ServerConfig.superNodeID) {
            resp = queryObjectMeta(req);
        } else {
            resp = (QueryObjectMetaResp) P2PUtils.requestBP(req, node);
        }
        return resp;
    }

    @Override
    public Object handle() throws Throwable {
        try {
            getSuperNodeId();
        } catch (NodeMgmtException e) {
            LOG.error("Invalid super node pubkey:" + this.getPublicKey());
            return new ServiceException(ServiceErrorCode.INVALID_NODE_ID, e.getMessage());
        }
        return queryObjectMeta(request);
    }

    private static QueryObjectMetaResp queryObjectMeta(QueryObjectMetaReq req) throws ServiceException {
        LOG.info("Query object meta:" + req.getUserID() + "/" + req.getVNU());
        List<ObjectRefer> refers = ReferCache.getRefersCache(req.getVNU(), req.getUserID());
        long len = ReferCache.getObjectLength(req.getVNU(), req.getUserID());
        QueryObjectMetaResp resp = new QueryObjectMetaResp(refers, len);
        return resp;
    }

}
