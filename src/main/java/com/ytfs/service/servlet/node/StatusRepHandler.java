package com.ytfs.service.servlet.node;

import com.ytfs.common.ServiceErrorCode;
import static com.ytfs.common.ServiceErrorCode.INVALID_NODE_ID;
import com.ytfs.common.ServiceException;
import com.ytfs.service.packet.StatusRepReq;
import com.ytfs.service.packet.StatusRepResp;
import com.ytfs.service.servlet.Handler;
import com.ytfs.service.servlet.bp.NodeStatSync;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.nodemgmt.core.vo.Node;
import java.util.List;
import org.apache.log4j.Logger;

public class StatusRepHandler extends Handler<StatusRepReq> {

    private static final Logger LOG = Logger.getLogger(StatusRepHandler.class);

    @Override
    public Object handle() throws Throwable {
        int nodeid;
        try {
            nodeid = this.getNodeId();
        } catch (NodeMgmtException e) {
            LOG.error("Invalid node pubkey:" + this.getPublicKey() + ",ID:" + request.getId());
            return new ServiceException(ServiceErrorCode.INVALID_NODE_ID, e.getMessage());
        }
        if (nodeid != request.getId()) {
            LOG.error("StatusRep Nodeid ERR:" + nodeid + "!=" + request.getId());
            return new ServiceException(ServiceErrorCode.INVALID_NODE_ID);
        }
        long l = System.currentTimeMillis();
        try {
            Node node = YottaNodeMgmt.updateNodeStatus(nodeid, request.getCpu(), request.getMemory(), request.getBandwidth(),
                    request.getMaxDataSpace(), request.getUsedSpace(), request.getAddrs(), request.isRelay(), request.getVersion());
            StatusRepResp resp = new StatusRepResp();
            resp.setProductiveSpace(node.getProductiveSpace());
            List<String> ls = node.getAddrs();
            if (ls != null && !ls.isEmpty()) {
                resp.setRelayUrl(ls.get(0));
            }
            List<String> addrs = request.getAddrs();
            node.setAddrs(addrs);
            NodeStatSync.updateNode(node);
            LOG.debug("StatusRep Node:" + request.getId() + ",take times " + (System.currentTimeMillis() - l) + " ms");
            SendSpotCheckTask.startUploadShard(this.getNode());
            return resp;
        } catch (NodeMgmtException e) {
            LOG.error("UpdateNodeStatus ERR:" + e.getMessage() + ",ID:" + request.getId() + ",take times " + (System.currentTimeMillis() - l) + " ms");
            return new ServiceException(INVALID_NODE_ID);
        }
    }

}
