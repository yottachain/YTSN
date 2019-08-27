package com.ytfs.service.servlet.user;

import static com.ytfs.common.ServiceErrorCode.NO_ENOUGH_NODE;
import static com.ytfs.common.ServiceErrorCode.TOO_MANY_SHARDS;
import com.ytfs.service.dao.User;
import com.ytfs.common.node.NodeManager;
import com.ytfs.service.servlet.CacheAccessor;
import com.ytfs.service.servlet.Handler;
import static com.ytfs.service.servlet.user.UploadBlockInitHandler.sign;
import com.ytfs.common.ServiceException;
import com.ytfs.service.packet.ShardNode;
import com.ytfs.service.packet.UploadBlockSubReq;
import com.ytfs.service.packet.UploadBlockSubResp;
import com.ytfs.service.servlet.ErrorNodeCache;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.nodemgmt.core.vo.Node;
import java.util.List;
import org.apache.log4j.Logger;

public class UploadBlockSubHandler extends Handler<UploadBlockSubReq> {

    private static final Logger LOG = Logger.getLogger(UploadBlockSubHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        if (request.getShardCount() > 255) {
            return new ServiceException(TOO_MANY_SHARDS);
        }
        CacheAccessor.getUploadObjectCache(user.getUserID(), request.getVNU());
        LOG.info("Upload block " + user.getUserID() + "/" + request.getVNU() + "/" + request.getVBI() + ",node count:" + request.getShardCount() + ",retry...");
        List<Integer> errid = request.getErrid();
        ErrorNodeCache.addErrorNode(errid);
        int count = UploadBlockInitHandler.incExcessCount(request.getShardCount());
        int[] errids = ErrorNodeCache.getErrorIds();
        Node[] nodes = NodeManager.getNode(count, errids);
        if (nodes.length != count) {
            LOG.warn("No enough data nodes:" + nodes.length + "/" + request.getShardCount());
            throw new ServiceException(NO_ENOUGH_NODE);
        }
        UploadBlockSubResp resp = new UploadBlockSubResp();
        setNodes(resp, nodes, request.getVBI());
        return resp;
    }

    private void setNodes(UploadBlockSubResp resp, Node[] ns, long VBI) throws NodeMgmtException {
        ShardNode[] nodes = new ShardNode[request.getShardCount()];
        ShardNode[] excessNodes = new ShardNode[ns.length - request.getShardCount()];
        resp.setNodes(nodes);
        resp.setExcessNodes(excessNodes);
        for (int ii = 0; ii < ns.length; ii++) {
            ShardNode snode = new ShardNode(ns[ii]);
            sign(snode, VBI);
            if (ii < nodes.length) {
                nodes[ii] = snode;
            } else {
                excessNodes[ii - nodes.length] = snode;
            }
        }
    }
}
