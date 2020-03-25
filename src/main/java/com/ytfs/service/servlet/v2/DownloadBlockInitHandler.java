package com.ytfs.service.servlet.v2;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.common.node.NodeManager;
import com.ytfs.service.dao.BlockAccessor;
import com.ytfs.service.dao.BlockMeta;
import com.ytfs.service.dao.ShardAccessor;
import com.ytfs.service.dao.ShardMeta;
import com.ytfs.service.dao.User;
import com.ytfs.service.packet.user.DownloadBlockDBResp;
import com.ytfs.service.packet.user.DownloadBlockInitResp;
import com.ytfs.service.packet.v2.DownloadBlockInitReqV2;
import com.ytfs.service.servlet.Handler;
import io.yottachain.nodemgmt.core.vo.Node;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

public class DownloadBlockInitHandler extends Handler<DownloadBlockInitReqV2> {

    private static final Logger LOG = Logger.getLogger(DownloadBlockInitHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser(request);
        if (user == null) {
            return new ServiceException(ServiceErrorCode.INVALID_SIGNATURE);
        }
        BlockMeta meta = BlockAccessor.getBlockMetaVNF(request.getVBI());
        LOG.info("Download block:" + request.getVBI() + " ,VNF " + meta.getVNF());
        if (meta.getVNF() == 0) {//存储在数据库
            byte[] dat = BlockAccessor.readBlockData(request.getVBI());
            DownloadBlockDBResp res = new DownloadBlockDBResp();
            res.setData(dat);
            return res;
        }
        DownloadBlockInitResp resp = new DownloadBlockInitResp();
        resp.setVNF(meta.getVNF());
        resp.setAR(meta.getAR());
        ShardMeta[] metas = ShardAccessor.getShardMeta(request.getVBI(), meta.getVNF());
        byte[][] VHF = new byte[metas.length][];
        int[] nodeids = new int[metas.length];
        List<Integer> nodeidsls = new ArrayList();
        for (int ii = 0; ii < metas.length; ii++) {
            nodeids[ii] = metas[ii].getNodeId();
            if (!nodeidsls.contains(metas[ii].getNodeId())) {
                nodeidsls.add(metas[ii].getNodeId());
            }
            VHF[ii] = metas[ii].getVHF();
        }
        resp.setVHF(VHF);
        List<Node> ls = NodeManager.getNode(nodeidsls);
        if (ls.size() != nodeidsls.size()) {
            LOG.warn("Some Nodes have been cancelled.");
        }
        Node[] ns = new Node[ls.size()];
        resp.setNodes(ls.toArray(ns));
        resp.setNodeids(nodeids);
        return resp;
    }

}
