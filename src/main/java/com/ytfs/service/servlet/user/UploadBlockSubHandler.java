package com.ytfs.service.servlet.user;

import static com.ytfs.common.ServiceErrorCode.NO_ENOUGH_NODE;
import com.ytfs.service.dao.User;
import com.ytfs.common.node.NodeManager;
import com.ytfs.service.servlet.CacheAccessor;
import com.ytfs.service.servlet.Handler;
import com.ytfs.service.servlet.UploadBlockCache;
import com.ytfs.service.servlet.UploadShardCache;
import static com.ytfs.service.servlet.user.UploadBlockInitHandler.sign;
import com.ytfs.common.ServiceException;
import static com.ytfs.common.conf.ServerConfig.Excess_Shard_Index;
import com.ytfs.service.packet.ShardNode;
import com.ytfs.service.packet.UploadBlockSubReq;
import com.ytfs.service.packet.UploadBlockSubResp;
import com.ytfs.service.packet.UploadShardRes;
import com.ytfs.service.servlet.ErrorNodeCache;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.nodemgmt.core.vo.Node;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

public class UploadBlockSubHandler extends Handler<UploadBlockSubReq> {

    private static final Logger LOG = Logger.getLogger(UploadBlockSubHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        UploadBlockCache cache = CacheAccessor.getUploadBlockCache(request.getVBI());
        UploadShardRes[] ress = request.getRes();
        int needExcess = 0;
        if (ress[0] != null && ress[0].getSHARDID() == Excess_Shard_Index) {
            needExcess = 1;
        }
        LOG.info("Upload block " + user.getUserID() + "/" + cache.getVNU() + ",Err count:" + (ress.length - needExcess) + ",retry...");
        List<UploadShardRes> fails = new ArrayList();
        Map<Integer, UploadShardCache> caches = cache.getShardCaches();
        List<Integer> errid_104 = new ArrayList();
        for (UploadShardRes res : ress) {
            if (res.getRES() == UploadShardRes.RES_OK) {
                continue;
            }
            UploadShardCache ca = caches.get(res.getSHARDID());
            if (ca != null && ca.getRes() == UploadShardRes.RES_OK) {
                continue;
            }
            fails.add(res);
            if (res.getRES() == UploadShardRes.RES_NETIOERR
                    || res.getRES() == UploadShardRes.RES_NO_SPACE) {
                ErrorNodeCache.addErrorNode(res.getNODEID());
            }
            if (res.getRES() == UploadShardRes.RES_REP_ERR) {
                if (!errid_104.contains(res.getNODEID())) {
                    errid_104.add(res.getNODEID());
                }
            }
        }
        UploadBlockSubResp resp = new UploadBlockSubResp();
        if (fails.isEmpty()) {
            return resp;
        }
        int count = needExcess == 1 ? UploadBlockInitHandler.incExcessCount(fails.size()) : fails.size();
        int[] errids = ErrorNodeCache.getErrorIds(errid_104);
        Node[] nodes = NodeManager.getNode(count, errids);
        if (nodes.length != count) {
            LOG.warn("No enough data nodes:" + nodes.length + "/" + fails.size());
            throw new ServiceException(NO_ENOUGH_NODE);
        } else {
            LOG.info("Assigned node:" + getAssignedNodeIDs(nodes) + ",Errid count:" + errids.length);
        }
        setNodes(resp, nodes, fails, request.getVBI(), fails.size());
        return resp;
    }

    private String getAssignedNodeIDs(Node[] nodes) {
        String res = null;
        for (Node n : nodes) {
            res = res == null ? ("[" + n.getId()) : (res + "," + n.getId());
        }
        return res + "]";
    }

    private void setNodes(UploadBlockSubResp resp, Node[] ns, List<UploadShardRes> fails, long VBI, int needcount) throws NodeMgmtException {
        ShardNode[] nodes = new ShardNode[ns.length];
        resp.setNodes(nodes);
        for (int ii = 0; ii < ns.length; ii++) {
            nodes[ii] = new ShardNode(ns[ii]);
            if (ii >= needcount) {
                nodes[ii].setShardid(Excess_Shard_Index);
            } else {
                nodes[ii].setShardid(fails.get(ii).getSHARDID());
            }
            sign(nodes[ii], VBI);
        }
    }
}
