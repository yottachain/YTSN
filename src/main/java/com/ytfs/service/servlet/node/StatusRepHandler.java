package com.ytfs.service.servlet.node;

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
        try {
            int nodeid = this.getNodeId();
            LOG.debug("StatusRep Node:" + nodeid);
            Node node = YottaNodeMgmt.updateNodeStatus(nodeid, request.getCpu(), request.getMemory(), request.getBandwidth(),
                    request.getMaxDataSpace(), request.getAddrs(), request.isRelay());
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
            SNSynchronizer.request(req, ServerConfig.superNodeID);
            return resp;
        } catch (NodeMgmtException e) {
            if (e.getMessage().contains("please register this node first")) {
                LOG.warn("Please register this node first,ID:" + request.getId());
                return new ServiceException(INVALID_NODE_ID);
            } else if (e.getMessage().contains("No result")) {
                LOG.warn("No result,ID:" + request.getId());
                return new ServiceException(INVALID_NODE_ID);
            } else {
                throw e;
            }
        }
    }

}
