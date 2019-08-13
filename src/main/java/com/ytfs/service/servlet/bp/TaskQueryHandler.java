package com.ytfs.service.servlet.bp;

import com.ytfs.common.Function;
import com.ytfs.common.ServiceErrorCode;
import static com.ytfs.common.ServiceErrorCode.SERVER_ERROR;
import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.node.NodeManager;
import com.ytfs.service.dao.ShardAccessor;
import com.ytfs.service.dao.ShardMeta;
import com.ytfs.service.packet.P2PLocation;
import com.ytfs.service.packet.TaskDescription;
import com.ytfs.service.packet.TaskDescriptionCP;
import com.ytfs.service.packet.TaskQueryReq;
import com.ytfs.service.servlet.Handler;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.nodemgmt.core.vo.Node;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

public class TaskQueryHandler extends Handler<TaskQueryReq> {

    private static final Logger LOG = Logger.getLogger(TaskQueryHandler.class);

    public static Object taskQueryCall(TaskQueryReq req, SuperNode node) throws ServiceException {
        if (node.getId() == ServerConfig.superNodeID) {
            try {
                return query(req);
            } catch (Throwable t) {
                LOG.error("Query task ERR:", t);
                throw t instanceof ServiceException ? (ServiceException) t : new ServiceException(SERVER_ERROR, t.getMessage());
            }
        } else {
            return P2PUtils.requestBP(req, node);
        }
    }

    private static Object query(TaskQueryReq req) throws Throwable {
        byte[] DNI = req.getDNI();
        int shardCount = (int) DNI[1];
        long VBI = Function.bytes2Integer(DNI, 2, 8);
        byte[] VHF = new byte[32];
        System.arraycopy(DNI, DNI.length - 32, VHF, 0, 32);
        ShardMeta[] metas = ShardAccessor.getShardMeta(VBI, shardCount);
        LOG.debug("Query shard meta return " + metas.length + "items,VBI:" + VBI);
        List<Integer> nodeidsls = new ArrayList();
        long VFI = 0;
        for (ShardMeta meta : metas) {
            if (!nodeidsls.contains(meta.getNodeId())) {
                nodeidsls.add(meta.getNodeId());
            }
            if (meta.getNodeId() == req.getNodeId() && Arrays.equals(meta.getVHF(), VHF)) {
                VFI = meta.getVFI();
            }
        }
        List<Node> ls = NodeManager.getNode(nodeidsls);
        if (ls.size() != nodeidsls.size()) {
            LOG.warn("Some Nodes have been cancelled.");
        }
        Map<Integer, Node> map = new HashMap();
        for (Node n : ls) {
            map.put(n.getId(), n);
        }
        byte[] id = new byte[42 + 12];
        System.arraycopy(Function.long2bytes(VFI), 0, id, 0, 8);
        System.arraycopy(Function.int2bytes(req.getNodeId()), 0, id, 8, 4);
        System.arraycopy(DNI, 0, id, 12, 42);
        List<byte[]> hashs = new ArrayList();
        List<P2PLocation> locations = new ArrayList();
        for (ShardMeta meta : metas) {
            hashs.add(meta.getVHF());
            P2PLocation location = new P2PLocation();
            Node node = map.get(meta.getNodeId());
            location.setAddrs(node.getAddrs());
            location.setNodeId(node.getNodeid());
            locations.add(location);
        }
        if (shardCount > UserConfig.Default_PND) {
            TaskDescription task = new TaskDescription();
            task.setId(id);
            task.setParityShardCount(UserConfig.Default_PND);
            task.setRecoverId((int) (VFI - VBI));
            task.setHashs(hashs);
            task.setLocations(locations);
            return task;
        } else {
            TaskDescriptionCP task = new TaskDescriptionCP();
            task.setId(id);
            task.setDataHash(VHF);
            task.setLocations(locations);
            return task;
        }
    }

    @Override
    public Object handle() throws Throwable {
        try {
            getSuperNodeId();
        } catch (NodeMgmtException e) {
            LOG.error("Invalid super node pubkey:" + this.getPublicKey());
            return new ServiceException(ServiceErrorCode.INVALID_NODE_ID, e.getMessage());
        }
        return query(request);
    }
}
