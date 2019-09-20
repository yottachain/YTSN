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
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.Sequence;
import com.ytfs.service.packet.ObjectRefer;
import com.ytfs.service.packet.bp.SaveObjectMetaReq;
import com.ytfs.service.packet.bp.SaveObjectMetaResp;
import com.ytfs.service.packet.user.UploadBlockEndReq;
import com.ytfs.service.packet.UploadShardRes;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.servlet.CacheAccessor;
import com.ytfs.service.servlet.bp.DNISenderPool;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.nodemgmt.core.vo.Node;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

public class UploadBlockEndHandler extends Handler<UploadBlockEndReq> {

    private static final Logger LOG = Logger.getLogger(UploadBlockEndHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        if (user == null) {
            return new ServiceException(ServiceErrorCode.NEED_LOGIN);
        }
        long l = System.currentTimeMillis();
        int userid = user.getUserID();
        LOG.debug("Receive UploadBlockEnd request:/" + request.getVNU() + "/" + request.getId());
        List<UploadShardRes> res = request.getOkList();
        if (res.size() > 160) {
            return new ServiceException(ServiceErrorCode.TOO_MANY_SHARDS);
        }
        BlockMeta bmeta = BlockAccessor.getBlockMeta(request.getVHP(), request.getVHB());
        if (bmeta != null) {
            LOG.warn(request.getVNU() + "/" + request.getId() + " already exist.");
            return new VoidResp();
        }
        long VBI = Sequence.generateBlockID(res.size());
        List<ShardMeta> ls = verify(request, res, VBI);
        ShardAccessor.saveShardMetas(ls);
        BlockMeta meta = makeBlockMeta(request, VBI, res.size());
        BlockAccessor.saveBlockMeta(meta);
        long starttime = System.currentTimeMillis();
        SaveObjectMetaReq saveObjectMetaReq = makeSaveObjectMetaReq(request, userid, meta.getVBI(), request.getVNU());
        saveObjectMetaReq.setNlink(1);
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

    private void sendDNI(List<ShardMeta> ls, long VBI) {
        ls.stream().forEach((doc) -> {
            int nid = doc.getNodeId();
            byte[] vhf = doc.getVHF();
            byte[] vbi = Function.long2bytes(VBI);
            byte[] data = new byte[vhf.length + 10];
            data[0] = (byte) ServerConfig.superNodeID;
            data[1] = (byte) ls.size();
            System.arraycopy(vbi, 0, data, 2, 8);
            System.arraycopy(vhf, 0, data, 10, vhf.length);
            DNISenderPool.startSender(data, nid, false);
        });
    }

    private SaveObjectMetaReq makeSaveObjectMetaReq(UploadBlockEndReq req, int userid, long vbi, ObjectId VNU) {
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

    private BlockMeta makeBlockMeta(UploadBlockEndReq req, long VBI, int shardCount) {
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
         }*/
    }

    private List<ShardMeta> verify(UploadBlockEndReq req, List<UploadShardRes> resList, long VBI) throws ServiceException, NoSuchAlgorithmException, NodeMgmtException {
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        List<ShardMeta> ls = new ArrayList();
        UploadShardRes[] shards = new UploadShardRes[resList.size()];
        // List<Integer> nodeidsls = new ArrayList();
        resList.stream().forEach((res) -> {
            shards[res.getSHARDID()] = res;
            //  nodeidsls.add(res.getNODEID());
        });
        /*
         List<Node> nodels = NodeManager.getNode(nodeidsls);
         if (ls.size() != nodeidsls.size()) {
         LOG.warn("Some Nodes have been cancelled.");
         throw new ServiceException(NO_ENOUGH_NODE);
         }
         Map<Integer, Node> map = new HashMap();
         nodels.stream().forEach((n) -> {
         map.put(n.getId(), n);
         });
         */
        for (int ii = 0; ii < shards.length; ii++) {
            UploadShardRes res = shards[ii];
            if (req.isRsShard()) {
                md5.update(res.getVHF());
            } else {
                if (ii == 0) {
                    md5.update(res.getVHF());
                }
            }
            if (!verifySign(res, null)) {
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
