package com.ytfs.service.servlet.bp;

import com.ytfs.common.conf.ServerConfig;
import com.ytfs.service.dao.ObjectAccessor;
import com.ytfs.service.dao.ObjectMeta;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.servlet.Handler;
import static com.ytfs.common.ServiceErrorCode.INVALID_UPLOAD_ID;
import com.ytfs.common.ServiceException;
import com.ytfs.service.packet.QueryObjectMetaReq;
import com.ytfs.service.packet.QueryObjectMetaResp;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

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
        return queryObjectMeta(request);
    }

    private static QueryObjectMetaResp queryObjectMeta(QueryObjectMetaReq req) throws ServiceException {
        LOG.info("Query object meta:" + req.getUserID() + "/" + req.getVNU());
        ObjectMeta meta = queryObjectMeta(req.getVNU(), req.getUserID());
        QueryObjectMetaResp resp = new QueryObjectMetaResp(meta.getBlocks(), meta.getLength());
        return resp;
    }

    static ObjectMeta queryObjectMeta(ObjectId VNU, int UserID) throws ServiceException {
        ObjectMeta meta = ObjectAccessor.getObject(VNU);
        if (meta == null) {
            throw new ServiceException(INVALID_UPLOAD_ID);
        } else {
            if (meta.getUserID() != UserID) {
                throw new ServiceException(INVALID_UPLOAD_ID);
            } else {
                return meta;
            }
        }
    }
}
