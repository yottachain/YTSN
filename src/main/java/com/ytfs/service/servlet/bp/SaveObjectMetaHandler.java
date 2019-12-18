package com.ytfs.service.servlet.bp;

import com.ytfs.common.ServiceErrorCode;
import static com.ytfs.common.ServiceErrorCode.INVALID_UPLOAD_ID;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.service.dao.ObjectAccessor;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.servlet.Handler;
import com.ytfs.common.ServiceException;
import com.ytfs.service.packet.bp.SaveObjectMetaReq;
import com.ytfs.service.packet.bp.SaveObjectMetaResp;
import com.ytfs.service.servlet.CacheAccessor;
import com.ytfs.service.servlet.UploadObjectCache;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import org.apache.log4j.Logger;

public class SaveObjectMetaHandler extends Handler<SaveObjectMetaReq> {
    
    private static final Logger LOG = Logger.getLogger(SaveObjectMetaHandler.class);

    /**
     * 保存上传进度至BPU
     *
     * @param req
     * @return
     * @throws ServiceException
     */
    public static SaveObjectMetaResp saveObjectMetaCall(SaveObjectMetaReq req) throws ServiceException {
        SuperNode node = SuperNodeList.getUserSuperNode(req.getUserID());
        if (node.getId() == ServerConfig.superNodeID) {
            return saveObjectMeta(req);
        } else {
            return (SaveObjectMetaResp) P2PUtils.requestBP(req, node);
        }
    }
    
    @Override
    public Object handle() throws Throwable {
        try {
            getSuperNodeId();
        } catch (NodeMgmtException e) {
            LOG.error("Invalid super node pubkey:" + this.getPublicKey());
            return new ServiceException(ServiceErrorCode.INVALID_NODE_ID, e.getMessage());
        }
        return saveObjectMeta(request);
    }
    
    private static SaveObjectMetaResp saveObjectMeta(SaveObjectMetaReq request) throws ServiceException {
        LOG.info("Save object meta:" + request.getUserID() + "/" + request.getVNU()
                + "/" + request.getRefer().getId() + "/" + request.getRefer().getVBI());
        if (request.getRefer().getVBI() == 0) {
            throw new ServiceException(INVALID_UPLOAD_ID);
        }
        SaveObjectMetaResp resp = new SaveObjectMetaResp();
        UploadObjectCache cache = CacheAccessor.getUploadObjectCache(request.getUserID(), request.getVNU());
        if (cache.exists(request.getRefer().getId())) {
            resp.setExists(true);
        } else {
            resp.setExists(false);
            byte[] bs = request.getRefer().toBytes();
            ObjectAccessor.updateObject(request.getUserID(), request.getVNU(), bs, request.getUsedSpace());
            cache.setBlockNum(request.getRefer().getId());
        }
        return resp;
    }
    
}
