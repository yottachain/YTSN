package com.ytfs.service.servlet.node;

import com.ytfs.common.Function;
import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.ShardAccessor;
import com.ytfs.service.packet.TaskOpResultList;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.servlet.Handler;
import com.ytfs.service.servlet.bp.DNISenderPool;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.p2phost.utils.Base58;
import java.util.List;
import org.apache.log4j.Logger;

public class TaskOpResultListHandler extends Handler<TaskOpResultList> {

    private static final Logger LOG = Logger.getLogger(TaskOpResultListHandler.class);

    @Override
    public Object handle() throws Throwable {
        int nodeid;
        try {
            nodeid = this.getNodeId();
        } catch (NodeMgmtException e) {
            LOG.error("Invalid node pubkey:" + this.getPublicKey());
            return new ServiceException(ServiceErrorCode.INVALID_NODE_ID, e.getMessage());
        }
        List<byte[]> ls = request.getId();
        int index = 0;
        if (ls == null || ls.isEmpty()) {
            LOG.error("TaskID list is empty.");
            return new VoidResp();
        } else {
            LOG.info("Received: count " + ls.size());
        }
        for (byte[] id : ls) {
            int res = request.getRES().get(index);
            index++;
            if (id == null || id.length != 54) {
                LOG.error("TaskID Length Less than 54.");
                continue;
            }
            if (res != 0) {
                LOG.error("Rebuild task " + Base58.encode(id) + " at " + nodeid + " execution failed:" + res);
                continue;
            }
            long VFI = Function.bytes2Integer(id, 0, 8);
            int rebuildNodeId = (int) Function.bytes2Integer(id, 8, 4);
            byte[] DNI = new byte[42];
            System.arraycopy(id, 12, DNI, 0, 42);
            byte[] VHF = new byte[32];
            System.arraycopy(DNI, DNI.length - 32, VHF, 0, 32);
            boolean b = ShardAccessor.updateShardMeta(VFI, nodeid, VHF);
            if (!b) {
                LOG.error("Rebuild task " + Base58.encode(id) + " update shard meta failed:" + VFI);
                continue;
            }
            DNISenderPool.startSender(DNI, nodeid, false);
            DNISenderPool.startSender(DNI, rebuildNodeId, true);
            LOG.debug("Rebuild OK:" + Base58.encode(id));
        }
        return new VoidResp();
    }

}
