package com.ytfs.service.servlet.bp;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.packet.bp.NodeSyncReq;

import com.ytfs.service.servlet.Handler;
import io.jafka.jeos.util.Base58;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.nodemgmt.core.vo.Node;
import java.util.Iterator;
import java.util.List;
import org.apache.log4j.Logger;

public class NodeSyncHandler extends Handler<NodeSyncReq> {

    private static final Logger LOG = Logger.getLogger(NodeSyncHandler.class);

    @Override
    public Object handle() throws Throwable {
        try {
            getSuperNodeId();
        } catch (NodeMgmtException e) {
            LOG.error("Invalid super node pubkey:" + this.getPublicKey());
            return new ServiceException(ServiceErrorCode.INVALID_NODE_ID, e.getMessage());
        }
        long starttime = System.currentTimeMillis();
        List<Node> ls = request.getNode();
        ls.stream().forEach((n) -> {
            try {
                YottaNodeMgmt.syncNode(n);
            } catch (NodeMgmtException ne) {
                LOG.error("Sync node " + n.getId() + " stat err:" + ne.getMessage());
            }
        });
        LOG.debug("Sync Node STAT,count:" + ls.size() + ",take times " + (System.currentTimeMillis() - starttime) + " ms");
        return new VoidResp();
    }

}
