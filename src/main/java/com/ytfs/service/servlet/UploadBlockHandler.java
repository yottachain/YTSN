package com.ytfs.service.servlet;

import com.ytfs.service.utils.ServiceException;
import com.ytfs.service.packet.UploadShardRes;
import com.ytfs.service.*;
import com.ytfs.service.codec.BlockEncrypted;
import com.ytfs.service.codec.KeyStoreCoder;
import com.ytfs.service.packet.ObjectRefer;
import com.ytfs.service.dao.*;
import com.ytfs.service.node.*;
import static com.ytfs.service.utils.ServiceErrorCode.*;
import com.ytfs.service.packet.*;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.nodemgmt.core.vo.Node;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.util.*;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;

public class UploadBlockHandler {

    private static final Logger LOG = Logger.getLogger(UploadBlockHandler.class);

    /**
     * 数据块上传完毕
     *
     * @param req
     * @param userid
     * @return OK
     * @throws ServiceException
     * @throws Throwable
     */
    static VoidResp complete(UploadBlockEndReq req, User user) throws ServiceException, Throwable {
        int userid = user.getUserID();
        UploadBlockCache cache = CacheAccessor.getUploadBlockCache(req.getVBI());
        UploadObjectCache progress = CacheAccessor.getUploadObjectCache(userid, cache.getVNU());
        Map<Integer, UploadShardCache> caches = cache.getShardCaches();
        List<Document> ls = verify(req, caches, cache.getShardcount(), req.getVBI());
        ShardAccessor.saveShardMetas(ls);
        BlockMeta meta = makeBlockMeta(req, req.getVBI(), cache.getShardcount());
        BlockAccessor.saveBlockMeta(meta);
        SaveObjectMetaReq saveObjectMetaReq = makeSaveObjectMetaReq(req, userid, meta.getVBI(), cache.getVNU());
        saveObjectMetaReq.setNlink(1);
        try {
            SaveObjectMetaResp resp = SuperReqestHandler.saveObjectMetaCall(saveObjectMetaReq);
            progress.setBlockNum(req.getId());
            if (resp.isExists()) {
                BlockAccessor.decBlockNLINK(meta);//-1
            }
        } catch (ServiceException r) {
            BlockAccessor.decBlockNLINK(meta);//-1
            throw r;
        }
        SendShard2SNM.sendShard2SNM(ls);
        LOG.info("Upload block:/" + cache.getVNU() + "/" + req.getVBI() + " OK!");
        return new VoidResp();
    }

    static SaveObjectMetaReq makeSaveObjectMetaReq(UploadBlockEndReq req, int userid, long vbi, ObjectId VNU) {
        SaveObjectMetaReq saveObjectMetaReq = new SaveObjectMetaReq();
        saveObjectMetaReq.setUserID(userid);
        saveObjectMetaReq.setVNU(VNU);
        ObjectRefer refer = new ObjectRefer();
        refer.setId(req.getId());
        refer.setKEU(req.getKEU());
        refer.setOriginalSize(req.getOriginalSize());
        refer.setRealSize(req.getRealSize());
        refer.setSuperID((byte) ServerConfig.superNodeID);
        refer.setVBI(vbi);
        saveObjectMetaReq.setRefer(refer);
        return saveObjectMetaReq;
    }

    static BlockMeta makeBlockMeta(UploadBlockEndReq req, long VBI, int shardCount) {
        BlockMeta meta = new BlockMeta();
        meta.setVBI(VBI);
        meta.setKED(req.getKED());
        meta.setNLINK(1);
        if (req.isRsShard()) {
            meta.setVNF(shardCount);
        } else {
            meta.setVNF(shardCount * -1);
        }
        meta.setVHB(req.getVHB());
        meta.setVHP(req.getVHP());
        return meta;
    }

    static List<Document> verify(UploadBlockEndReq req, Map<Integer, UploadShardCache> caches, int shardCount, long vbi) throws ServiceException {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] vhf = null;
            List<Document> ls = new ArrayList();
            for (int ii = 0; ii < shardCount; ii++) {
                UploadShardCache cache = caches.get(ii);
                if (cache == null || !(cache.getRes() == UploadShardRes.RES_OK
                        || cache.getRes() == UploadShardRes.RES_VNF_EXISTS)) {
                    throw new ServiceException(INVALID_SHARD);
                }
                if (req.isRsShard()) {
                    md5.update(cache.getVHF());
                } else {
                    if (ii == 0) {
                        md5.update(cache.getVHF());
                        vhf = cache.getVHF();
                    } else {
                        if (!Arrays.equals(cache.getVHF(), vhf)) {
                            throw new ServiceException(INVALID_SHARD);
                        }
                    }
                }
                ShardMeta meta = new ShardMeta(vbi + ii, cache.getNodeid(), cache.getVHF());
                ls.add(meta.toDocument());
            }
            byte[] vhb = md5.digest();
            if (!Arrays.equals(vhb, req.getVHB())) {
                throw new ServiceException(INVALID_VHB);
            }
            if (req.getVHP() == null || req.getVHP().length != 32) {
                throw new ServiceException(INVALID_VHP);
            }
            if (req.getKEU() == null || req.getKEU().length != 32) {
                throw new ServiceException(INVALID_KEU);
            }
            if (req.getKED() == null || req.getKED().length != 32) {
                throw new ServiceException(INVALID_KED);
            }
            return ls;
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    /**
     * 将数据块小于PL2的数据块写入数据库
     *
     * @param req
     * @param userid
     * @return OK
     * @throws ServiceException
     * @throws Throwable
     */
    static VoidResp saveToDB(UploadBlockDBReq req, User user) throws ServiceException, Throwable {
        int userid = user.getUserID();
        LOG.info("Upload block " + user.getUserID() + "/" + req.getVNU() + "/" + req.getId() + " to DB...");
        UploadObjectCache progress = CacheAccessor.getUploadObjectCache(userid, req.getVNU());
        if (progress.exists(req.getId())) {
            return new VoidResp();
        }
        SuperNode n = SuperNodeList.getBlockSuperNode(req.getVHP());
        if (n.getId() != ServerConfig.superNodeID) {//验证数据块是否对应
            throw new ServiceException(ILLEGAL_VHP_NODEID);
        }
        BlockEncrypted b = new BlockEncrypted();
        b.setData(req.getData());
        verify(req, b.getVHB());
        BlockMeta meta = makeBlockMeta(req, Sequence.generateBlockID(1));
        BlockAccessor.saveBlockData(meta.getVBI(), req.getData());
        BlockAccessor.saveBlockMeta(meta);
        SaveObjectMetaReq saveObjectMetaReq = makeSaveObjectMetaReq(req, userid, meta.getVBI());
        saveObjectMetaReq.setNlink(1);
        try {
            SaveObjectMetaResp resp = SuperReqestHandler.saveObjectMetaCall(saveObjectMetaReq);
            progress.setBlockNum(req.getId());
            if (resp.isExists()) {
                BlockAccessor.decBlockNLINK(meta);//-1
            }
        } catch (ServiceException r) {
            BlockAccessor.decBlockNLINK(meta);//-1
            throw r;
        }
        return new VoidResp();
    }

    static void verify(UploadBlockDBReq req, byte[] vhb) throws ServiceException {
        if (!Arrays.equals(vhb, req.getVHB())) {
            throw new ServiceException(INVALID_VHB);
        }
        if (req.getVHP() == null || req.getVHP().length != 32) {
            throw new ServiceException(INVALID_VHP);
        }
        if (req.getKEU() == null || req.getKEU().length != 32) {
            throw new ServiceException(INVALID_KEU);
        }
        if (req.getKED() == null || req.getKED().length != 32) {
            throw new ServiceException(INVALID_KED);
        }
        if (req.getData().length > ServerConfig.PL2 * 2) {
            throw new ServiceException(TOO_BIG_BLOCK);
        }
    }

    static BlockMeta makeBlockMeta(UploadBlockDBReq req, long VBI) {
        BlockMeta meta = new BlockMeta();
        meta.setVBI(VBI);
        meta.setKED(req.getKED());
        meta.setNLINK(1);
        meta.setVNF(0);
        meta.setVHB(req.getVHB());
        meta.setVHP(req.getVHP());
        return meta;
    }

    static SaveObjectMetaReq makeSaveObjectMetaReq(UploadBlockDBReq req, int userid, long vbi) {
        SaveObjectMetaReq saveObjectMetaReq = new SaveObjectMetaReq();
        saveObjectMetaReq.setUserID(userid);
        saveObjectMetaReq.setVNU(req.getVNU());
        ObjectRefer refer = new ObjectRefer();
        refer.setId(req.getId());
        refer.setKEU(req.getKEU());
        refer.setOriginalSize(req.getOriginalSize());
        refer.setRealSize(req.getData().length);
        refer.setSuperID((byte) ServerConfig.superNodeID);
        refer.setVBI(vbi);
        saveObjectMetaReq.setRefer(refer);
        return saveObjectMetaReq;
    }

    /**
     * 重复的数据块,引用计数+1,写用户元数据
     *
     * @param req
     * @param userid
     * @return OK
     * @throws ServiceException
     * @throws Throwable
     */
    static VoidResp repeat(UploadBlockDupReq req, User user) throws ServiceException, Throwable {
        int userid = user.getUserID();
        LOG.info("Upload block " + user.getUserID() + "/" + req.getVNU() + "/" + req.getId() + " exist...");
        UploadObjectCache progress = CacheAccessor.getUploadObjectCache(userid, req.getVNU());
        if (progress.exists(req.getId())) {
            return new VoidResp();
        }
        BlockMeta meta = BlockAccessor.getBlockMeta(req.getVHP(), req.getVHB());
        if (meta == null) {
            throw new ServiceException(NO_SUCH_BLOCK);
        }
        verify(req);
        BlockAccessor.incBlockNLINK(meta);//+1        
        SaveObjectMetaReq saveObjectMetaReq = makeSaveObjectMetaReq(req, userid, meta.getVBI());
        saveObjectMetaReq.setNlink(meta.getNLINK() + 1);
        try {
            SaveObjectMetaResp resp = SuperReqestHandler.saveObjectMetaCall(saveObjectMetaReq);
            progress.setBlockNum(req.getId());
            if (resp.isExists()) {
                BlockAccessor.decBlockNLINK(meta);//-1
            }
        } catch (ServiceException r) {
            BlockAccessor.decBlockNLINK(meta);//-1
            throw r;
        }
        return new VoidResp();
    }

    static void verify(UploadBlockDupReq req) throws ServiceException {
        if (req.getKEU() == null || req.getKEU().length != 32) {
            throw new ServiceException(INVALID_KEU);
        }
    }

    static SaveObjectMetaReq makeSaveObjectMetaReq(UploadBlockDupReq req, int userid, long vbi) {
        SaveObjectMetaReq saveObjectMetaReq = new SaveObjectMetaReq();
        saveObjectMetaReq.setUserID(userid);
        saveObjectMetaReq.setVNU(req.getVNU());
        ObjectRefer refer = new ObjectRefer();
        refer.setId(req.getId());
        refer.setKEU(req.getKEU());
        refer.setOriginalSize(req.getOriginalSize());
        refer.setRealSize(req.getRealSize());
        refer.setSuperID((byte) ServerConfig.superNodeID);
        saveObjectMetaReq.setRefer(refer);
        return saveObjectMetaReq;
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
    static Object init(UploadBlockInitReq req, User user) throws ServiceException, Throwable {
        int userid = user.getUserID();
        LOG.info("Upload block init " + user.getUserID() + "/" + req.getVNU() + "/" + req.getId());
        if (req.getShardCount() > 255) {
            throw new ServiceException(TOO_MANY_SHARDS);
        }
        SuperNode n = SuperNodeList.getBlockSuperNode(req.getVHP());
        if (n.getId() != ServerConfig.superNodeID) {//验证数据块是否对应
            throw new ServiceException(ILLEGAL_VHP_NODEID);
        }
        UploadObjectCache progress = CacheAccessor.getUploadObjectCache(userid, req.getVNU());
        UploadBlockInitResp resp = new UploadBlockInitResp();
        if (progress.exists(req.getId())) {
            return new VoidResp();
        }
        if (req instanceof UploadBlockInit2Req) {
            distributeNode(req, resp, user.getKUEp());
            return resp;
        }
        List<BlockMeta> ls = BlockAccessor.getBlockMeta(req.getVHP());
        if (ls.isEmpty()) {
            distributeNode(req, resp, user.getKUEp());
            return resp;
        } else {
            UploadBlockDupResp resp2 = new UploadBlockDupResp();
            setKEDANDVHB(resp2, ls);
            return resp2;
        }
    }

    static void setKEDANDVHB(UploadBlockDupResp resp, List<BlockMeta> ls) {
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
     * 请求补发数据分片
     *
     * @param req
     * @param userid
     * @return
     * @throws ServiceException
     * @throws Throwable
     */
    static UploadBlockSubResp subUpload(UploadBlockSubReq req, User user) throws ServiceException, Throwable {
        UploadBlockCache cache = CacheAccessor.getUploadBlockCache(req.getVBI());
        LOG.info("Upload block " + user.getUserID() + "/" + cache.getVNU() + " retry...");
        List<UploadShardRes> fails = new ArrayList();
        Map<Integer, UploadShardCache> caches = cache.getShardCaches();
        UploadShardRes[] ress = req.getRes();
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
        setNodes(resp, nodes, fails, req.getVBI(), cache);
        return resp;
    }

    //分配节点
    private static void distributeNode(UploadBlockInitReq req, UploadBlockInitResp resp, byte[] userkey) throws Exception {
        if (req.getShardCount() > 0) {//需要数据库
            Node[] nodes = NodeManager.getNode(req.getShardCount());
            if (nodes.length != req.getShardCount()) {
                throw new ServiceException(SERVER_ERROR);
            }
            long blockid = Sequence.generateBlockID(req.getShardCount());
            setNodes(resp, nodes, blockid);
            UploadBlockCache cache = new UploadBlockCache(nodes, req.getShardCount(), req.getVNU());
            cache.setUserKey(userkey);
            CacheAccessor.addUploadBlockCache(blockid, cache);
        }
    }

    private static void setNodes(UploadBlockInitResp resp, Node[] ns, long VBI) throws NodeMgmtException {
        resp.setVBI(VBI);
        ShardNode[] nodes = new ShardNode[ns.length];
        resp.setNodes(nodes);
        for (int ii = 0; ii < ns.length; ii++) {
            nodes[ii] = new ShardNode(ii, ns[ii]);
            sign(nodes[ii], VBI);
        }
    }

    private static void sign(ShardNode sn, long VBI) {
        byte[] nid = sn.getKey().getBytes(Charset.forName("utf-8"));
        ByteBuffer buf = ByteBuffer.allocate(nid.length + 8);
        buf.put(nid);
        buf.putLong(VBI);
        buf.flip();
        byte[] signed = KeyStoreCoder.ecdsaSign(buf.array(), ServerConfig.SNDSP);
        sn.setSign(signed);
    }

    private static void setNodes(UploadBlockSubResp resp, Node[] ns, List<UploadShardRes> fails, long VBI, UploadBlockCache cache) throws NodeMgmtException {
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
