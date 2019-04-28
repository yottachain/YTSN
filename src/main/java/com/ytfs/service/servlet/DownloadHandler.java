package com.ytfs.service.servlet;

import com.ytfs.service.dao.BlockAccessor;
import com.ytfs.service.dao.ObjectAccessor;
import com.ytfs.service.dao.ObjectMeta;
import com.ytfs.service.dao.ShardAccessor;
import com.ytfs.service.dao.ShardMeta;
import com.ytfs.service.dao.User;
import com.ytfs.service.node.NodeManager;
import com.ytfs.service.packet.DownloadBlockDBResp;
import com.ytfs.service.packet.DownloadBlockInitReq;
import com.ytfs.service.packet.DownloadBlockInitResp;
import com.ytfs.service.packet.DownloadObjectInitReq;
import com.ytfs.service.packet.DownloadObjectInitResp;
import com.ytfs.service.packet.ServiceException;
import io.yottachain.nodemgmt.core.vo.Node;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

public class DownloadHandler {

    private static final Logger LOG = Logger.getLogger(DownloadHandler.class);

    /**
     * 获取对象引用块meta
     *
     * @param req
     * @param userid
     * @return
     * @throws ServiceException
     * @throws Throwable
     */
    static DownloadObjectInitResp init(DownloadObjectInitReq req, User user) throws ServiceException, Throwable {
        int userid = user.getUserID();
        ObjectMeta meta = ObjectAccessor.getObject(userid, req.getVHW());
        LOG.info("Download object:" + userid + "/" + meta.getVNU());
        DownloadObjectInitResp resp = new DownloadObjectInitResp();
        resp.setRefers(meta.getBlocks());
        resp.setLength(meta.getLength());
        return resp;
    }

    /**
     * 获取对象引用块meta
     *
     * @param req
     * @param userid
     * @return
     * @throws ServiceException
     * @throws Throwable
     */
    static Object getBlockMeta(DownloadBlockInitReq req, User user) throws ServiceException, Throwable {
        int vnf = BlockAccessor.getBlockMetaVNF(req.getVBI());
        LOG.info("Download block:" + req.getVBI() + " ,VNF " + vnf);
        if (vnf == 0) {//存储在数据库
            byte[] dat = BlockAccessor.readBlockData(req.getVBI());
            DownloadBlockDBResp res = new DownloadBlockDBResp();
            res.setData(dat);
            return res;
        }
        DownloadBlockInitResp resp = new DownloadBlockInitResp();
        resp.setVNF(vnf);
        int len = vnf > 0 ? vnf : (vnf * -1);
        ShardMeta[] metas = ShardAccessor.getShardMeta(req.getVBI(), len);
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
        Node[] ns = new Node[ls.size()];
        resp.setNodes(ls.toArray(ns));
        resp.setNodeids(nodeids);
        return resp;
    }
}
