package com.ytfs.service.servlet.user;

import com.ytfs.service.dao.User;
import com.ytfs.service.node.NodeManager;
import com.ytfs.service.packet.ShardNode;
import com.ytfs.service.packet.UploadBlockSubReq;
import com.ytfs.service.packet.UploadBlockSubResp;
import com.ytfs.service.packet.UploadShardRes;
import com.ytfs.service.servlet.CacheAccessor;
import com.ytfs.service.servlet.Handler;
import com.ytfs.service.servlet.UploadBlockCache;
import com.ytfs.service.servlet.UploadShardCache;
import static com.ytfs.service.servlet.user.UploadBlockInitHandler.sign;
import static com.ytfs.service.ServiceErrorCode.SERVER_ERROR;
import com.ytfs.service.ServiceException;
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
        LOG.info("Upload block " + user.getUserID() + "/" + cache.getVNU() + " retry...");
        List<UploadShardRes> fails = new ArrayList();
        Map<Integer, UploadShardCache> caches = cache.getShardCaches();
        UploadShardRes[] ress = request.getRes();
        for (UploadShardRes res : ress) {
            if (res.getRES() == UploadShardRes.RES_OK) {
                continue;
            }
            UploadShardCache ca = caches.get(res.getSHARDID());
            if (ca == null) {
                if (res.getRES() == UploadShardRes.RES_NETIOERR) {//需要惩罚节点
                    if (cache.getNodes()[res.getSHARDID()] == res.getNODEID()) {
                        NodeManager.punishNode(res.getNODEID());
                    }
                }
                fails.add(res);
            } else {
                if (ca.getRes() == UploadShardRes.RES_OK) {
                    continue;
                }
                if (res.getRES() == UploadShardRes.RES_NO_SPACE && res.getRES() == ca.getRes()) {
                    NodeManager.noSpace(res.getNODEID());
                }
                fails.add(res);
            }
        }
        UploadBlockSubResp resp = new UploadBlockSubResp();
        if (fails.isEmpty()) {
            return resp;
        }
        Node[] nodes = NodeManager.getNode(fails.size());
        if (nodes.length != fails.size()) {
            throw new ServiceException(SERVER_ERROR);
        }
        setNodes(resp, nodes, fails, request.getVBI(), cache);
        return resp;
    }

    private void setNodes(UploadBlockSubResp resp, Node[] ns, List<UploadShardRes> fails, long VBI, UploadBlockCache cache) throws NodeMgmtException {
        ShardNode[] nodes = new ShardNode[ns.length];
        resp.setNodes(nodes);
        for (int ii = 0; ii < ns.length; ii++) {
            nodes[ii] = new ShardNode(ns[ii]);
            nodes[ii].setShardid(fails.get(ii).getSHARDID());
            sign(nodes[ii], VBI);
            cache.getNodes()[fails.get(ii).getSHARDID()] = nodes[ii].getNodeId();//更新缓存
        }
    }
}
