package com.ytfs.service.servlet.user;

import com.ytfs.common.conf.ServerConfig;
import com.ytfs.service.dao.BlockAccessor;
import com.ytfs.service.dao.BlockMeta;
import com.ytfs.service.dao.ShardAccessor;
import com.ytfs.service.dao.ShardMeta;
import com.ytfs.service.dao.User;
import com.ytfs.service.servlet.CacheAccessor;
import com.ytfs.service.servlet.Handler;
import com.ytfs.service.servlet.UploadBlockCache;
import com.ytfs.service.servlet.UploadObjectCache;
import com.ytfs.service.servlet.UploadShardCache;
import com.ytfs.service.servlet.bp.SaveObjectMetaHandler;
import static com.ytfs.common.ServiceErrorCode.INVALID_KED;
import static com.ytfs.common.ServiceErrorCode.INVALID_KEU;
import static com.ytfs.common.ServiceErrorCode.INVALID_SHARD;
import static com.ytfs.common.ServiceErrorCode.INVALID_VHB;
import static com.ytfs.common.ServiceErrorCode.INVALID_VHP;
import com.ytfs.common.ServiceException;
import com.ytfs.service.packet.ObjectRefer;
import com.ytfs.service.packet.SaveObjectMetaReq;
import com.ytfs.service.packet.SaveObjectMetaResp;
import com.ytfs.service.packet.UploadBlockEndReq;
import com.ytfs.service.packet.UploadShardRes;
import com.ytfs.service.packet.VoidResp;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;

public class UploadBlockEndHandler extends Handler<UploadBlockEndReq> {

    private static final Logger LOG = Logger.getLogger(UploadBlockEndHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        int userid = user.getUserID();
        UploadBlockCache cache = CacheAccessor.getUploadBlockCache(request.getVBI());
        UploadObjectCache progress = CacheAccessor.getUploadObjectCache(userid, cache.getVNU());
        Map<Integer, UploadShardCache> caches = cache.getShardCaches();
        List<Document> ls = verify(request, caches, cache.getShardcount(), request.getVBI());
        ShardAccessor.saveShardMetas(ls);
        BlockMeta meta = makeBlockMeta(request, request.getVBI(), cache.getShardcount());
        BlockAccessor.saveBlockMeta(meta);
        SaveObjectMetaReq saveObjectMetaReq = makeSaveObjectMetaReq(request, userid, meta.getVBI(), cache.getVNU());
        saveObjectMetaReq.setNlink(1);
        try {
            SaveObjectMetaResp resp = SaveObjectMetaHandler.saveObjectMetaCall(saveObjectMetaReq);
            progress.setBlockNum(request.getId());
            if (resp.isExists()) {
                BlockAccessor.decBlockNLINK(meta);//-1
            }
        } catch (ServiceException r) {
            BlockAccessor.decBlockNLINK(meta);//-1
            throw r;
        }
        SendShard2SNM.sendShard2SNM(ls);
        LOG.info("Upload block:/" + cache.getVNU() + "/" + request.getVBI() + " OK!");
        return new VoidResp();
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

    private List<Document> verify(UploadBlockEndReq req, Map<Integer, UploadShardCache> caches, int shardCount, long vbi) throws ServiceException {
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

}
