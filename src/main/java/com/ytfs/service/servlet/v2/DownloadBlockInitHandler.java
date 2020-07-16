package com.ytfs.service.servlet.v2;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
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
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.nodemgmt.core.vo.Node;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

public class DownloadBlockInitHandler extends Handler<DownloadBlockInitReqV2> {

    private static final Logger LOG = Logger.getLogger(DownloadBlockInitHandler.class);

    public static Cache<Integer, Node> Node_CACHE = CacheBuilder.newBuilder()
            .expireAfterWrite(30, TimeUnit.MINUTES)
            .maximumSize(20000)
            .build();

    public static List<Node> getNodes(List<Integer> nodeidsls) throws NodeMgmtException {
        List<Node> lss = new ArrayList();
        List<Integer> newnodeidsls = new ArrayList();
        for (int id : nodeidsls) {
            Node n = Node_CACHE.getIfPresent(id);
            if (n == null) {
                newnodeidsls.add(id);
            } else {
                lss.add(n);
            }
        }
        if (!newnodeidsls.isEmpty()) {
            List<Node> ls = NodeManager.getNode(newnodeidsls);
            for (Node n : ls) {
                Node_CACHE.put(n.getId(), n);
                lss.add(n);
            }
        }
        return lss;
    }

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
        List<Node> lss = new ArrayList();
        List<Integer> newnodeidsls = new ArrayList();
        for (int id : nodeidsls) {
            Node n = Node_CACHE.getIfPresent(id);
            if (n == null) {
                newnodeidsls.add(id);
            } else {
                lss.add(n);
            }
        }
        if (!newnodeidsls.isEmpty()) {
            List<Node> ls = NodeManager.getNode(newnodeidsls);
            for (Node n : ls) {
                Node_CACHE.put(n.getId(), n);
                lss.add(n);
            }
        }
        Node[] ns = new Node[lss.size()];
        resp.setNodes(lss.toArray(ns));
        resp.setNodeids(nodeids);
        return resp;
    }

}
