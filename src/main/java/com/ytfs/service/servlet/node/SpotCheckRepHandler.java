package com.ytfs.service.servlet.node;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.service.packet.SpotCheckStatus;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.servlet.Handler;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import java.util.List;
import org.apache.log4j.Logger;

public class SpotCheckRepHandler extends Handler<SpotCheckStatus> {
    
    private static final Logger LOG = Logger.getLogger(SpotCheckRepHandler.class);
    
    @Override
    public Object handle() throws Throwable {
        try {
            getNodeId();
        } catch (NodeMgmtException e) {
            LOG.error("Invalid node pubkey:" + this.getPublicKey());
            return new ServiceException(ServiceErrorCode.INVALID_NODE_ID, e.getMessage());
        }
        try {
            String taskid = request.getTaskId();
            List<Integer> ls = request.getInvalidNodeList();
            if (ls == null || ls.isEmpty()) {
                LOG.warn("SpotCheckTaskStatus:" + taskid + ",invalidNodeList is empty.");
            } else {
                LOG.info("SpotCheckTaskStatus:" + taskid);
                int[] nodes = new int[ls.size()];
                for (int ii = 0; ii < nodes.length; ii++) {
                    nodes[ii] = ls.get(ii);
                }
                YottaNodeMgmt.updateTaskStatus(taskid, nodes);
            }
        } catch (NodeMgmtException e) {
            LOG.error("", e);
        }
        return new VoidResp();
    }
    
}
