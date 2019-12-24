package com.ytfs.service.servlet.node;

import com.ytfs.common.Function;
import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.dao.CacheBaseAccessor;
import com.ytfs.service.dao.ShardAccessor;
import com.ytfs.service.packet.TaskOpResultList;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.servlet.Handler;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import io.yottachain.p2phost.utils.Base58;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.types.Binary;

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
        List<Document> docs = new ArrayList();
        for (byte[] id : ls) {
            int res = request.getRES().get(index);
            index++;
            if (id == null || id.length != 39) {
                LOG.error("TaskID Length Less than 39.");
                continue;
            }
            if (res != 0) {
                //LOG.error("Rebuild task " + Base58.encode(id) + " at " + nodeid + " execution failed:" + res);
                continue;
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
                continue;
            }
            SuperNode sn = SuperNodeList.getDNISuperNode(nodeid);
            if (ServerConfig.superNodeID == sn.getId()) {
                try {
                    YottaNodeMgmt.addDNI(nodeid, DNI);
                } catch (NodeMgmtException r) {
                    if (!(r.getMessage() != null && r.getMessage().contains("duplicate key"))) {
                        LOG.error("InsertDNI " + nodeid + "-[" + io.jafka.jeos.util.Base58.encode(DNI) + "] ERR:" + r.getMessage());
                        continue;
                    }
                }
            } else {
                Document adddoc = new Document();
                adddoc.append("nodeId", nodeid);
                adddoc.append("vhf", new Binary(DNI));
                adddoc.append("delete", false);
                docs.add(adddoc);
            }
            sn = SuperNodeList.getDNISuperNode(rebuildNodeId);
            if (ServerConfig.superNodeID == sn.getId()) {
                try {
                    YottaNodeMgmt.deleteDNI(rebuildNodeId, DNI);
                } catch (NodeMgmtException r) {
                    LOG.error("DeleteDNI " + rebuildNodeId + "-[" + io.jafka.jeos.util.Base58.encode(DNI) + "] ERR:" + r.getMessage());
                    continue;
                }
            } else {
                Document adddoc = new Document();
                adddoc.append("nodeId", rebuildNodeId);
                adddoc.append("vhf", new Binary(DNI));
                adddoc.append("delete", true);
                docs.add(adddoc);
            }
            LOG.debug("Rebuild OK:" + Base58.encode(id));
        }
        if (!docs.isEmpty()) {
            CacheBaseAccessor.addDNI(docs);
        }
        return new VoidResp();
    }

}
