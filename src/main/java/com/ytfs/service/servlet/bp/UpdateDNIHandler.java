package com.ytfs.service.servlet.bp;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.packet.bp.UpdateDNIMutiReq;
import com.ytfs.service.packet.bp.UpdateDNIReq;
import com.ytfs.service.servlet.Handler;
import io.jafka.jeos.util.Base58;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import java.util.List;
import org.apache.log4j.Logger;

public class UpdateDNIHandler extends Handler<UpdateDNIMutiReq> {

    private static final Logger LOG = Logger.getLogger(UpdateDNIMutiReq.class);

    @Override
    public Object handle() throws Throwable {
        try {
            getSuperNodeId();
        } catch (NodeMgmtException e) {
            LOG.error("Invalid super node pubkey:" + this.getPublicKey());
            return new ServiceException(ServiceErrorCode.INVALID_NODE_ID, e.getMessage());
        }
        long starttime = System.currentTimeMillis();
        List<UpdateDNIReq> ls = request.getList();
        for (UpdateDNIReq req : ls) {
            if (req.isDelete()) {
                try {
                    YottaNodeMgmt.deleteDNI(req.getNodeid(), req.getDni());
                } catch (NodeMgmtException ne) {
                    LOG.error("DeleteDNI " + req.getNodeid() + "-[" + Base58.encode(req.getDni()) + "] ERR:" + ne.getMessage());
                    return new ServiceException(ServiceErrorCode.SERVER_ERROR);
                }
            } else {
                try {
                    YottaNodeMgmt.addDNI(req.getNodeid(), req.getDni());
                } catch (NodeMgmtException r) {
                    if (!(r.getMessage() != null && r.getMessage().contains("duplicate key"))) {
                        LOG.error("InsertDNI " + req.getNodeid() + "-[" + Base58.encode(req.getDni()) + "] ERR:" + r.getMessage());
                        return new ServiceException(ServiceErrorCode.SERVER_ERROR);
                    }
                }
            }
        }
        LOG.info("Update DNI OK,count:" + ls.size() + ",take times " + (System.currentTimeMillis() - starttime) + " ms");
        return new VoidResp();
    }

}
