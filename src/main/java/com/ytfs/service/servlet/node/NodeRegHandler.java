package com.ytfs.service.servlet.node;

import com.ytfs.service.servlet.Handler;
import static com.ytfs.common.ServiceErrorCode.NODE_EXISTS;
import com.ytfs.common.ServiceException;
import com.ytfs.service.packet.NodeRegReq;
import com.ytfs.service.packet.NodeRegResp;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.nodemgmt.core.vo.Node;
import org.apache.log4j.Logger;

public class NodeRegHandler extends Handler<NodeRegReq> {

    private static final Logger LOG = Logger.getLogger(NodeRegHandler.class);

    @Override
    public Object handle() throws Throwable {
        LOG.info("Reg Node:" + request.getNodeid());
        try {
            Node node = YottaNodeMgmt.registerNode(request.getNodeid(),
                    this.getPublicKey(), request.getOwner(),
                    request.getMaxDataSpace(), request.getAddrs());
            NodeRegResp resp = new NodeRegResp();
            resp.setId(node.getId());
            resp.setAssignedSpace(node.getAssignedSpace());
            LOG.info("Node Registered,Id:" + node.getId());
            return resp;
        } catch (NodeMgmtException e) {
            if (e.getMessage().contains("multiple write")) {
                return new ServiceException(NODE_EXISTS);
            } else {
                throw e;
            }
        }
    }

}
