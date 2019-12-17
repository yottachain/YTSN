package com.ytfs.service.servlet.node;

import com.ytfs.common.Function;
import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.dao.CacheBaseAccessor;
import com.ytfs.service.dao.ShardAccessor;
import com.ytfs.service.packet.TaskOpResult;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.servlet.Handler;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import io.yottachain.p2phost.utils.Base58;
import org.apache.log4j.Logger;

public class TaskOpResultHandler extends Handler<TaskOpResult> {

    private static final Logger LOG = Logger.getLogger(TaskOpResultHandler.class);

    @Override
    public Object handle() throws Throwable {
        int nodeid;
        try {
            nodeid = this.getNodeId();
        } catch (NodeMgmtException e) {
            LOG.error("Invalid node pubkey:" + this.getPublicKey());
            return new ServiceException(ServiceErrorCode.INVALID_NODE_ID, e.getMessage());
        }
        long l = System.currentTimeMillis();
        byte[] id = this.request.getId();
        if (id == null || id.length != 39) {
            LOG.error("TaskID Length Less than 39.");
            return new VoidResp();
        }
        if (request.getRES() != 0) {
            LOG.error("Rebuild task " + Base58.encode(id) + " at " + nodeid + " execution failed:" + request.getRES());
            return new VoidResp();
        }
        long VFI = Function.bytes2Integer(id, 0, 8);
        int rebuildNodeId = (int) Function.bytes2Integer(id, 8, 4);
        byte[] DNI = new byte[27];
        System.arraycopy(id, 12, DNI, 0, 27);
        byte[] VHF = new byte[16];
        System.arraycopy(DNI, DNI.length - 16, VHF, 0, 16);
        boolean b = ShardAccessor.updateShardMeta(VFI, nodeid, VHF);
        if (!b) {
            LOG.error("Rebuild task " + Base58.encode(id) + " update shard meta failed:" + VFI);
            return new VoidResp();
        }
        SuperNode sn = SuperNodeList.getDNISuperNode(nodeid);
        if (ServerConfig.superNodeID == sn.getId()) {
            try {
                YottaNodeMgmt.addDNI(nodeid, DNI);
            } catch (NodeMgmtException r) {
                if (!(r.getMessage() != null && r.getMessage().contains("duplicate key"))) {
                    LOG.error("InsertDNI " + nodeid + "-[" + io.jafka.jeos.util.Base58.encode(DNI) + "] ERR:" + r.getMessage());
                    return new VoidResp();
                }
            }
        } else {
            CacheBaseAccessor.addDNI(nodeid, DNI, false);
        }
        sn = SuperNodeList.getDNISuperNode(rebuildNodeId);
        if (ServerConfig.superNodeID == sn.getId()) {
            try {
                YottaNodeMgmt.deleteDNI(rebuildNodeId, DNI);
            } catch (NodeMgmtException r) {
                LOG.error("DeleteDNI " + rebuildNodeId + "-[" + io.jafka.jeos.util.Base58.encode(DNI) + "] ERR:" + r.getMessage());
                return new VoidResp();
            }
        } else {
            CacheBaseAccessor.addDNI(rebuildNodeId, DNI, true);
        }
        LOG.debug("Rebuild OK: " + Base58.encode(id) + ",take times " + (System.currentTimeMillis() - l) + " ms");
        return new VoidResp();
    }

}
