package com.ytfs.service.servlet.bp;

import com.ytfs.common.Function;
import com.ytfs.common.SerializationUtil;
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
import com.ytfs.service.packet.TaskDispatchList;
import com.ytfs.service.packet.TaskList;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.servlet.Handler;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.nodemgmt.core.vo.Node;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import io.yottachain.p2phost.utils.Base58;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

public class TaskListHandler extends Handler<TaskDispatchList> {

    private static final Logger LOG = Logger.getLogger(TaskListHandler.class);

    public static Object taskDispatchCall(TaskDispatchList req, SuperNode node) throws ServiceException {
        if (node.getId() == ServerConfig.superNodeID) {
            try {
                return dispatch(req);
            } catch (Throwable t) {
                LOG.error("", t);
                throw t instanceof ServiceException ? (ServiceException) t : new ServiceException(SERVER_ERROR, t.getMessage());
            }
        } else {
            return P2PUtils.requestBP(req, node);
        }
    }

    private static byte[] query(byte[] DNI, int nodeid) throws Throwable {
        int shardCount = (int) DNI[1] & 0xFF;
        long VBI = Function.bytes2Integer(DNI, 2, 8);
        byte[] VHF = new byte[32];
        System.arraycopy(DNI, DNI.length - 32, VHF, 0, 32);
        ShardMeta[] metas = ShardAccessor.getShardMeta(VBI, shardCount);
        List<Integer> nodeidsls = new ArrayList();
        long VFI = 0;
        for (ShardMeta meta : metas) {
            if (!nodeidsls.contains(meta.getNodeId())) {
                nodeidsls.add(meta.getNodeId());
            }
            if (meta.getNodeId() == nodeid && Arrays.equals(meta.getVHF(), VHF)) {
                VFI = meta.getVFI();
            }
        }
        List<Node> ls = NodeManager.getNode(nodeidsls);
        if (ls.size() != nodeidsls.size()) {
            LOG.error("Some Nodes have been cancelled.");
        }
        Map<Integer, Node> map = new HashMap();
        ls.stream().forEach((n) -> {
            map.put(n.getId(), n);
        });
        byte[] id = new byte[42 + 12];
        System.arraycopy(Function.long2bytes(VFI), 0, id, 0, 8); //VFI
        System.arraycopy(Function.int2bytes(nodeid), 0, id, 8, 4);
        System.arraycopy(DNI, 0, id, 12, 42);
        List<byte[]> hashs = new ArrayList();
        List<P2PLocation> locations = new ArrayList();
        for (ShardMeta meta : metas) {
            hashs.add(meta.getVHF());
            P2PLocation location = new P2PLocation();
            Node node = map.get(meta.getNodeId());
            if (node != null) {
                location.setAddrs(node.getAddrs());
                location.setNodeId(node.getNodeid());
            }
            locations.add(location);
        }
        Object task;
        if (shardCount > UserConfig.Default_PND) {
            task = new TaskDescription();
            ((TaskDescription) task).setId(id);
            ((TaskDescription) task).setParityShardCount(UserConfig.Default_PND);
            ((TaskDescription) task).setRecoverId((int) (VFI - VBI));
            ((TaskDescription) task).setHashs(hashs);
            ((TaskDescription) task).setLocations(locations);
        } else {
            task = new TaskDescriptionCP();
            ((TaskDescriptionCP) task).setId(id);
            ((TaskDescriptionCP) task).setDataHash(VHF);
            ((TaskDescriptionCP) task).setLocations(locations);
        }
        return SerializationUtil.serialize(task);
    }

    private static VoidResp dispatch(TaskDispatchList req) throws Throwable {
        Node execnode = NodeManager.getNode(req.getExecNodeId());
        if (execnode == null) {
            LOG.error("Node not found：" + req.getExecNodeId());
            return new VoidResp();
        }
        TaskList task = new TaskList();
        List<byte[]> ls = req.getDNI();
        ls.stream().forEach((dni) -> {
            try {
                byte[] bs = query(dni, req.getNodeId());
                task.addTasks(bs);
            } catch (Throwable t) {
                LOG.error("Query shard meta [" + Base58.encode(dni) + "] err:", t);
            }
        });
        if (!(task.getTasks() == null || task.getTasks().isEmpty())) {
            try {
                P2PUtils.requestNode(task, execnode);
                LOG.debug("Send rebuild task total " + task.getTasks().size() + " to " + req.getExecNodeId());
            } catch (Throwable ex) {
                LOG.error("Send rebuild tasks to " + req.getExecNodeId() + " ERR:" + ex.getMessage());
            }
        }
        return new VoidResp();
    }

    @Override
    public Object handle() throws Throwable {
        try {
            getSuperNodeId();
        } catch (NodeMgmtException e) {
            LOG.error("Invalid super node pubkey:" + this.getPublicKey());
            return new ServiceException(ServiceErrorCode.INVALID_NODE_ID, e.getMessage());
        }
        try {
            dispatch(request);
        } catch (Throwable t) {
            LOG.error("Dispatch task ERR:", t);
        }
        return new VoidResp();
    }

}
