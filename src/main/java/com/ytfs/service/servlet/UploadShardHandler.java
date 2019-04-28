package com.ytfs.service.servlet;

import com.ytfs.service.ServerConfig;
import static com.ytfs.service.packet.UploadShardRes.RES_BAD_REQUEST;
import com.ytfs.service.dao.User;
import com.ytfs.service.dao.UserCache;
import com.ytfs.service.eos.EOSClient;
import com.ytfs.service.packet.ServiceErrorCode;
import static com.ytfs.service.packet.ServiceErrorCode.TOO_MANY_SHARDS;
import com.ytfs.service.packet.ServiceException;
import com.ytfs.service.packet.UploadShardResp;
import com.ytfs.service.packet.VoidResp;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import org.apache.log4j.Logger;

public class UploadShardHandler {
    
    private static final Logger LOG = Logger.getLogger(UploadShardHandler.class);
    
    static void verify(UploadShardResp resp, byte[] key, int maxshardCount, int nodeid) throws ServiceException, NoSuchAlgorithmException, InvalidKeyException, SignatureException {
        if (resp.getSHARDID() >= maxshardCount) {
            throw new ServiceException(TOO_MANY_SHARDS);
        }//使用用户公钥验签
        /*
        Key pkey = KeyStoreCoder.rsaPublicKey(key);
        java.security.Signature signetcheck = java.security.Signature.getInstance("DSA");
        signetcheck.initVerify((PublicKey) pkey);
        signetcheck.update(resp.getVHF());
        signetcheck.update(Function.int2bytes(resp.getSHARDID()));
        signetcheck.update(Function.int2bytes(nodeid));
        signetcheck.update(Function.long2bytes(resp.getVBI()));
        if (!signetcheck.verify(resp.getUSERSIGN())) {
            throw new ServiceException(INVALID_SIGNATURE);
        }*/
    }

    /**
     * BPD收到存储节点的存储反馈
     *
     * @param resp
     * @param nodeid
     * @return
     * @throws ServiceException
     * @throws Throwable
     */
    static VoidResp uploadShardResp(UploadShardResp resp, int nodeid) throws ServiceException, Throwable {
        try {
            UploadBlockCache cache = CacheAccessor.getUploadBlockCache(resp.getVBI());
            User user = UserCache.getUser(cache.getUserKey());
            if (user == null) {
                throw new ServiceException(ServiceErrorCode.INVALID_USER_ID);
            }
            verify(resp, user.getKUEp(), cache.getShardcount(), nodeid);
            if (cache.getNodes()[resp.getSHARDID()] != nodeid) {
                throw new ServiceException(ServiceErrorCode.INVALID_NODE_ID);
            }
            if (resp.getRES() == RES_BAD_REQUEST) {
                long failtimes = CacheAccessor.getUploadBlockINC(resp.getVBI());
                if (failtimes >= ServerConfig.PNF) {
                    UploadObjectCache objcache = CacheAccessor.getUploadObjectCache(nodeid, cache.getVNU());
                    EOSClient eos = new EOSClient(user.getEosName());
                    eos.punishHDD(objcache.getFilesize());
                    CacheAccessor.clearCache(cache.getVNU(), resp.getVBI());//清除缓存
                    return new VoidResp();
                }
            }
            UploadShardCache shardCache = new UploadShardCache();
            shardCache.setNodeid(nodeid);
            shardCache.setRes(resp.getRES());
            shardCache.setVHF(resp.getVHF());
            shardCache.setShardid(resp.getSHARDID());
            CacheAccessor.addUploadShardCache(shardCache, resp.getVBI());          
        } catch (Exception e) {
            LOG.error("", e);
        }
        return new VoidResp();
    }
    
}
