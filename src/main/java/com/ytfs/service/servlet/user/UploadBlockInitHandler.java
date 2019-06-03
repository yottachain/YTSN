package com.ytfs.service.servlet.user;

import com.ytfs.common.conf.ServerConfig;
import com.ytfs.service.dao.BlockAccessor;
import com.ytfs.service.dao.BlockMeta;
import com.ytfs.service.dao.Sequence;
import com.ytfs.service.dao.User;
import com.ytfs.common.node.NodeManager;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.servlet.CacheAccessor;
import com.ytfs.service.servlet.Handler;
import com.ytfs.service.servlet.UploadBlockCache;
import com.ytfs.service.servlet.UploadObjectCache;
import static com.ytfs.common.ServiceErrorCode.ILLEGAL_VHP_NODEID;
import static com.ytfs.common.ServiceErrorCode.NO_ENOUGH_NODE;
import static com.ytfs.common.ServiceErrorCode.TOO_MANY_SHARDS;
import com.ytfs.common.ServiceException;
import com.ytfs.service.packet.ShardNode;
import com.ytfs.service.packet.UploadBlockDupResp;
import com.ytfs.service.packet.UploadBlockInit2Req;
import com.ytfs.service.packet.UploadBlockInitReq;
import com.ytfs.service.packet.UploadBlockInitResp;
import com.ytfs.service.packet.VoidResp;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.nodemgmt.core.vo.Node;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import org.apache.log4j.Logger;

public class UploadBlockInitHandler extends Handler<UploadBlockInitReq> {
    
    private static final Logger LOG = Logger.getLogger(UploadBlockInitHandler.class);
    
    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        int userid = user.getUserID();
        LOG.info("Upload block init " + user.getUserID() + "/" + request.getVNU() + "/" + request.getId());
        if (request.getShardCount() > 255) {
            throw new ServiceException(TOO_MANY_SHARDS);
        }
        SuperNode n = SuperNodeList.getBlockSuperNode(request.getVHP());
        if (n.getId() != ServerConfig.superNodeID) {//验证数据块是否对应
            throw new ServiceException(ILLEGAL_VHP_NODEID);
        }
        UploadObjectCache progress = CacheAccessor.getUploadObjectCache(userid, request.getVNU());
        UploadBlockInitResp resp = new UploadBlockInitResp();
        if (progress.exists(request.getId())) {
            return new VoidResp();
        }
        if (request instanceof UploadBlockInit2Req) {
            distributeNode(request, resp, user.getKUEp());
            return resp;
        }
        List<BlockMeta> ls = BlockAccessor.getBlockMeta(request.getVHP());
        if (ls.isEmpty()) {
            distributeNode(request, resp, user.getKUEp());
            return resp;
        } else {
            UploadBlockDupResp resp2 = new UploadBlockDupResp();
            setKEDANDVHB(resp2, ls);
            return resp2;
        }
    }

    /**
     * 检查数据块是否重复,或强制请求分配node
     *
     * @param req
     * @param userid
     * @return 0已上传 1重复 2未上传
     * @throws ServiceException
     * @throws Throwable
     */
    private void setKEDANDVHB(UploadBlockDupResp resp, List<BlockMeta> ls) {
        byte[][] VHB = new byte[ls.size()][];
        byte[][] KED = new byte[ls.size()][];
        int index = 0;
        for (BlockMeta m : ls) {
            VHB[index] = m.getVHB();
            KED[index] = m.getKED();
            index++;
        }
        resp.setVHB(VHB);
        resp.setKED(KED);
    }

    /**
     * 分配节点
     *
     * @param req
     * @param resp
     * @param userkey
     * @throws Exception
     */
    private void distributeNode(UploadBlockInitReq req, UploadBlockInitResp resp, byte[] userkey) throws Exception {
        if (req.getShardCount() > 0) {//需要数据库
            Node[] nodes = NodeManager.getNode(req.getShardCount());
            if (nodes.length != req.getShardCount()) {
                LOG.warn("No enough data nodes:" + nodes.length + "/" + req.getShardCount());
                throw new ServiceException(NO_ENOUGH_NODE);
            }
            long blockid = Sequence.generateBlockID(req.getShardCount());
            setNodes(resp, nodes, blockid);
            UploadBlockCache cache = new UploadBlockCache(nodes, req.getShardCount(), req.getVNU());
            cache.setUserKey(userkey);
            CacheAccessor.addUploadBlockCache(blockid, cache);
        }
    }
    
    private void setNodes(UploadBlockInitResp resp, Node[] ns, long VBI) throws NodeMgmtException {
        resp.setVBI(VBI);
        ShardNode[] nodes = new ShardNode[ns.length];
        resp.setNodes(nodes);
        for (int ii = 0; ii < ns.length; ii++) {
            nodes[ii] = new ShardNode(ii, ns[ii]);
            sign(nodes[ii], VBI);
        }
    }

    /**
     * 超级节点签名
     *
     * @param sn
     * @param VBI
     */
    static void sign(ShardNode sn, long VBI) {
        try {
            byte[] nid = sn.getKey().getBytes(Charset.forName("utf-8"));
            ByteBuffer buf = ByteBuffer.allocate(nid.length + 8);
            buf.put(nid);
            buf.putLong(VBI);
            buf.flip();
            String ss = io.yottachain.ytcrypto.YTCrypto.sign(ServerConfig.privateKey, buf.array());
            byte[] signed = ss.getBytes("UTF-8");
            sn.setSign(signed);
        } catch (Exception r) {
            LOG.error("", r);
        }
    }
}
