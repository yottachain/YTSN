package com.ytfs.service.servlet.user;

import com.ytfs.common.Function;
import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.service.dao.BlockAccessor;
import com.ytfs.service.dao.BlockMeta;
import com.ytfs.service.dao.ShardAccessor;
import com.ytfs.service.dao.ShardMeta;
import com.ytfs.service.dao.User;
import com.ytfs.service.servlet.Handler;
import com.ytfs.service.servlet.bp.SaveObjectMetaHandler;
import static com.ytfs.common.ServiceErrorCode.INVALID_KED;
import static com.ytfs.common.ServiceErrorCode.INVALID_KEU;
import static com.ytfs.common.ServiceErrorCode.INVALID_SIGNATURE;
import static com.ytfs.common.ServiceErrorCode.INVALID_VHB;
import static com.ytfs.common.ServiceErrorCode.INVALID_VHP;
import static com.ytfs.common.ServiceErrorCode.NO_ENOUGH_NODE;
import com.ytfs.common.ServiceException;
import com.ytfs.common.codec.ShardEncoder;
import static com.ytfs.common.conf.UserConfig.Default_PND;
import static com.ytfs.common.conf.UserConfig.Max_Shard_Count;
import com.ytfs.common.node.NodeManager;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.dao.CacheBaseAccessor;
import com.ytfs.service.packet.ObjectRefer;
import com.ytfs.service.packet.bp.SaveObjectMetaReq;
import com.ytfs.service.packet.bp.SaveObjectMetaResp;
import com.ytfs.service.packet.UploadShardRes;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.packet.user.UploadBlockEndSyncReq;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.nodemgmt.core.vo.Node;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.types.Binary;

public class UploadBlockEndSyncHandler extends Handler<UploadBlockEndSyncReq> {

    private static final Logger LOG = Logger.getLogger(UploadBlockEndSyncHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        if (user == null) {
            return new ServiceException(ServiceErrorCode.NEED_LOGIN);
        }
        long l = System.currentTimeMillis();
        int userid = user.getUserID();
        LOG.debug("Receive UploadBlockEndSyncHandler request:/" + request.getVNU() + "/" + request.getId());
        List<UploadShardRes> res = request.getOkList();
        if (res.size() > Max_Shard_Count + Default_PND) {
            return new ServiceException(ServiceErrorCode.TOO_MANY_SHARDS);
        }
        long VBI = request.getVBI();
        BlockMeta bmeta = BlockAccessor.getBlockMeta(VBI);
        if (bmeta == null) {
            return new VoidResp();
        }
        List<ShardMeta> ls = verify(request, res, VBI);
        ShardAccessor.saveShardMetas(ls);
        BlockMeta meta = makeBlockMeta(VBI, res.size());
        BlockAccessor.saveBlockMeta(meta);
        long starttime = System.currentTimeMillis();
        SaveObjectMetaReq saveObjectMetaReq = makeSaveObjectMetaReq(userid, VBI);
        saveObjectMetaReq.setUsedSpace(ServerConfig.PFL * res.size());
        try {
            SaveObjectMetaResp resp = SaveObjectMetaHandler.saveObjectMetaCall(saveObjectMetaReq);
            if (resp.isExists()) {
                LOG.warn("Block " + user.getUserID() + "/" + request.getVNU() + "/" + request.getId() + " has been uploaded.");
            }
        } catch (ServiceException r) {
            throw r;
        }
        LOG.info("Save object refer:/" + request.getVNU() + "/" + request.getId() + " OK,take times " + (System.currentTimeMillis() - starttime) + " ms");
        sendDNI(ls, VBI);
        LOG.info("Upload block:/" + request.getVNU() + "/" + request.getId() + " OK,take times " + (System.currentTimeMillis() - l) + " ms");
        return new VoidResp();
    }

    private void sendDNI(List<ShardMeta> ls, long VBI) throws Throwable {
        int AR = request.getAR();
        List<Document> docs = new ArrayList();
        for (ShardMeta doc : ls) {
            int nid = doc.getNodeId();
            byte[] vhf = doc.getVHF();
            byte[] vbi = Function.long2bytes(VBI);
            byte[] data = new byte[vhf.length + 11];
            data[0] = (byte) ServerConfig.superNodeID;
            data[1] = (byte) ls.size();
            data[2] = (byte) AR;
            System.arraycopy(vbi, 0, data, 3, 8);
            System.arraycopy(vhf, 0, data, 11, vhf.length);
            SuperNode sn = SuperNodeList.getDNISuperNode(nid);
            if (ServerConfig.superNodeID == sn.getId()) {
                try {
                    YottaNodeMgmt.addDNI(nid, data);
                } catch (NodeMgmtException r) {
                    if (!(r.getMessage() != null && r.getMessage().contains("duplicate key"))) {
                        throw r;
                    }
                }
            } else {
                Document adddoc = new Document();
                adddoc.append("nodeId", nid);
                adddoc.append("vhf", new Binary(data));
                adddoc.append("delete", false);
                docs.add(adddoc);
            }
        }
        if (!docs.isEmpty()) {
            CacheBaseAccessor.addDNI(docs);
        }
    }

    private SaveObjectMetaReq makeSaveObjectMetaReq(int userid, long vbi) {
        SaveObjectMetaReq saveObjectMetaReq = new SaveObjectMetaReq();
        saveObjectMetaReq.setUserID(userid);
        saveObjectMetaReq.setVNU(request.getVNU());
        ObjectRefer refer = new ObjectRefer();
        refer.setId(request.getId());
        refer.setKEU(request.getKEU());
        refer.setOriginalSize(request.getOriginalSize());
        refer.setRealSize(request.getRealSize());
        refer.setSuperID((byte) ServerConfig.superNodeID);
        refer.setVBI(vbi);
        saveObjectMetaReq.setRefer(refer);
        return saveObjectMetaReq;
    }

    private BlockMeta makeBlockMeta(long VBI, int shardCount) {
        BlockMeta meta = new BlockMeta();
        meta.setVBI(VBI);
        meta.setKED(request.getKED());
        meta.setNLINK(1);
        meta.setVNF(shardCount);
        meta.setAR(request.getAR());
        meta.setVHB(request.getVHB());
        meta.setVHP(request.getVHP());
        return meta;
    }

    private boolean verifySign(UploadShardRes res, Node node) {
        return true;
        /*
         try {
             LOG.info("PUBKEY:"+node.getPubkey());
             LOG.info("DNSIGN:"+res.getDNSIGN());
             return io.yottachain.ytcrypto.YTCrypto.verify(node.getPubkey(), res.getVHF(), res.getDNSIGN());
         } catch (YTCryptoException ex) {
             LOG.error("ERR:",ex);
             return false;
         }
         */
    }

    private List<ShardMeta> verify(UploadBlockEndSyncReq req, List<UploadShardRes> resList, long VBI) throws ServiceException, NoSuchAlgorithmException, NodeMgmtException {
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        List<ShardMeta> ls = new ArrayList();
        UploadShardRes[] shards = new UploadShardRes[resList.size()];
        List<Integer> nodeidsls = new ArrayList();
        resList.stream().forEach((res) -> {
            shards[res.getSHARDID()] = res;
            if (!nodeidsls.contains(res.getNODEID())) {
                nodeidsls.add(res.getNODEID());
            }
        });
        List<Node> nodels = NodeManager.getNode(nodeidsls);
        if (nodels.size() != nodeidsls.size()) {
            LOG.warn("Some Nodes have been cancelled.");
            throw new ServiceException(NO_ENOUGH_NODE);
        }
        Map<Integer, Node> map = new HashMap();
        nodels.stream().forEach((n) -> {
            map.put(n.getId(), n);
        });
        for (int ii = 0; ii < shards.length; ii++) {
            UploadShardRes res = shards[ii];
            if (req.getAR() != ShardEncoder.AR_COPY_MODE) {
                md5.update(res.getVHF());
            } else {
                if (ii == 0) {
                    md5.update(res.getVHF());
                }
            }
            if (!verifySign(res, map.get(res.getNODEID()))) {
                throw new ServiceException(INVALID_SIGNATURE);
            }
            ShardMeta meta = new ShardMeta(VBI + ii, res.getNODEID(), res.getVHF());
            ls.add(meta);
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
    }
}
