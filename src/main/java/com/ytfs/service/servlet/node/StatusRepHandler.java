package com.ytfs.service.servlet.node;

import com.ytfs.common.ServiceErrorCode;
import static com.ytfs.common.ServiceErrorCode.INVALID_NODE_ID;
import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.service.SNSynchronizer;
import com.ytfs.service.packet.StatusRepReq;
import com.ytfs.service.packet.StatusRepResp;
import com.ytfs.service.packet.bp.NodeSyncReq;
import com.ytfs.service.servlet.Handler;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.nodemgmt.core.vo.Node;
import java.util.List;
import org.apache.log4j.Logger;

public class StatusRepHandler extends Handler<StatusRepReq> {

    private static final Logger LOG = Logger.getLogger(StatusRepHandler.class);

    @Override
    public Object handle() throws Throwable {
        LOG.debug("StatusRep Node:" + request.getId());
        int nodeid;
        try {
            nodeid = this.getNodeId();
        } catch (NodeMgmtException e) {
            LOG.error("Invalid node pubkey:" + this.getPublicKey());
            return new ServiceException(ServiceErrorCode.INVALID_NODE_ID, e.getMessage());
        }
        if (nodeid != request.getId()) {
            LOG.error("StatusRep Nodeid ERR:" + nodeid + "!=" + request.getId());
            return new ServiceException(ServiceErrorCode.INVALID_NODE_ID);
        }
        try {
            Node node = YottaNodeMgmt.updateNodeStatus(nodeid, request.getCpu(), request.getMemory(), request.getBandwidth(),
                    request.getMaxDataSpace(), request.getAddrs(), request.isRelay(), request.getVersion());
            StatusRepResp resp = new StatusRepResp();
            resp.setProductiveSpace(node.getProductiveSpace());
            List<String> ls = node.getAddrs();
            if (ls != null && !ls.isEmpty()) {
                resp.setRelayUrl(ls.get(0));
            }
            List<String> addrs = request.getAddrs();
            node.setAddrs(addrs);
            NodeSyncReq req = new NodeSyncReq();
            req.setNode(node);
            SNSynchronizer.ayncRequest(req, ServerConfig.superNodeID);
            return resp;
        } catch (NodeMgmtException e) {
            LOG.error("UpdateNodeStatus ERR:" + e.getMessage() + ",ID:" + request.getId());
            return new ServiceException(INVALID_NODE_ID);
        }
    }

}
