package com.ytfs.service.servlet;

import com.ytfs.service.ServerConfig;
import com.ytfs.service.codec.KeyStoreCoder;
import static com.ytfs.service.packet.UploadShardRes.RES_BAD_REQUEST;
import com.ytfs.service.dao.User;
import com.ytfs.service.dao.UserCache;
import com.ytfs.service.utils.ServiceErrorCode;
import static com.ytfs.service.utils.ServiceErrorCode.INVALID_SIGNATURE;
import static com.ytfs.service.utils.ServiceErrorCode.TOO_MANY_SHARDS;
import com.ytfs.service.utils.ServiceException;
import com.ytfs.service.packet.UploadShardResp;
import com.ytfs.service.packet.VoidResp;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;

public class UploadShardHandler {

    private static final Logger LOG = Logger.getLogger(UploadShardHandler.class);

    //验证用户签名
    static void verify(UploadShardResp resp, byte[] key, int maxshardCount, int nodeid) throws ServiceException, NoSuchAlgorithmException, InvalidKeyException, SignatureException {
        if (resp.getSHARDID() >= maxshardCount) {
            throw new ServiceException(TOO_MANY_SHARDS);
        }
        ByteBuffer buf = ByteBuffer.allocate(48);
        buf.put(resp.getVHF());
        buf.putInt(resp.getSHARDID());
        buf.putInt(nodeid);
        buf.putLong(resp.getVBI());
        buf.flip();        
        if (!KeyStoreCoder.ecdsaVerify(buf.array(), resp.getUSERSIGN(), key)) {
            LOG.info(resp.getSHARDID() +" getUSERSIGN " + Hex.encodeHexString(resp.getUSERSIGN()));
           // throw new ServiceException(INVALID_SIGNATURE);
        }else{
            LOG.info(resp.getSHARDID() +" getUSERSIGN " + Hex.encodeHexString(resp.getUSERSIGN()));
        }
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
            LOG.debug("Upload shard " + resp.getVBI() + "/" + resp.getSHARDID() + "/" + resp.getRES());
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
                long failtimes = cache.errInc();
                if (failtimes >= ServerConfig.PNF) {
                    CacheAccessor.delUploadBlockCache(resp.getVBI());//清除缓存
                    return new VoidResp();
                }
            }
            UploadShardCache shardCache = new UploadShardCache();
            shardCache.setNodeid(nodeid);
            shardCache.setRes(resp.getRES());
            shardCache.setVHF(resp.getVHF());
            shardCache.setShardid(resp.getSHARDID());
            cache.addUploadShardCache(shardCache);
        } catch (Exception e) {
            LOG.error("", e);
        }
        return new VoidResp();
    }

}
