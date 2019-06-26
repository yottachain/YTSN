package com.ytfs.service.servlet.node;

import com.ytfs.service.servlet.Handler;
import static com.ytfs.common.ServiceErrorCode.NODE_EXISTS;
import com.ytfs.common.ServiceException;
import com.ytfs.service.packet.NodeRegReq;
import com.ytfs.service.packet.NodeRegResp;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.nodemgmt.core.vo.Node;
import java.util.List;
import org.apache.log4j.Logger;

public class NodeRegHandler extends Handler<NodeRegReq> {

    private static final Logger LOG = Logger.getLogger(NodeRegHandler.class);

    @Override
    public Object handle() throws Throwable {
        LOG.info("Reg Node:" + request.getNodeid());
        try {
            Node node = YottaNodeMgmt.registerNode(request.getId(), request.getNodeid(),
                    this.getPublicKey(), request.getOwner(),
                    request.getMaxDataSpace(), request.getAddrs(), request.isRelay());
            NodeRegResp resp = new NodeRegResp();
            resp.setId(node.getId());
            resp.setAssignedSpace(node.getAssignedSpace());
            List<String> ls = node.getAddrs();
            if (ls != null && !ls.isEmpty()) {
                resp.setRelayUrl(ls.get(0));
            }
            LOG.info("Node Registered,Id:" + node.getId());
            return resp;
        } catch (NodeMgmtException e) {
            if (e.getMessage().contains("multiple write")) {
                LOG.warn("Nodes exist,ID:" + request.getNodeid());
                return new ServiceException(NODE_EXISTS);
            } else {
                throw e;
            }
        }
    }

}
