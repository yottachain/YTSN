package com.ytfs.service.servlet.node;

import com.ytfs.common.conf.ServerConfig;
import com.ytfs.service.dao.User;
import com.ytfs.service.dao.UserCache;
import com.ytfs.service.servlet.Handler;
import com.ytfs.service.servlet.UploadBlockCache;
import com.ytfs.service.servlet.UploadShardCache;
import com.ytfs.common.ServiceErrorCode;
import static com.ytfs.common.ServiceErrorCode.TOO_MANY_SHARDS;
import com.ytfs.common.ServiceException;
import static com.ytfs.service.packet.UploadShardRes.RES_BAD_REQUEST;
import com.ytfs.service.packet.UploadShardResp;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.servlet.CacheAccessor;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import org.apache.log4j.Logger;

public class UploadShardHandler extends Handler<UploadShardResp> {

    private static final Logger LOG = Logger.getLogger(UploadShardHandler.class);

    @Override
    public Object handle() throws Throwable {
        try {
            int nodeid = this.getNodeId();
            LOG.debug("Upload shard " + request.getVBI() + "/" + request.getSHARDID() + "/" + request.getRES());
            UploadBlockCache cache = CacheAccessor.getUploadBlockCache(request.getVBI());
            User user = UserCache.getUser(cache.getUserKey());
            if (user == null) {
                throw new ServiceException(ServiceErrorCode.INVALID_USER_ID);
            }
            verify(request, user.getKUEp(), cache.getShardcount(), nodeid);
            if (cache.getNodes()[request.getSHARDID()] != nodeid) {
                throw new ServiceException(ServiceErrorCode.INVALID_NODE_ID);
            }
            if (request.getRES() == RES_BAD_REQUEST) {
                long failtimes = cache.errInc();
                if (failtimes >= ServerConfig.PNF) {
                    CacheAccessor.delUploadBlockCache(request.getVBI());//清除缓存
                    return new VoidResp();
                }
            }
            UploadShardCache shardCache = new UploadShardCache();
            shardCache.setNodeid(nodeid);
            shardCache.setRes(request.getRES());
            shardCache.setVHF(request.getVHF());
            shardCache.setShardid(request.getSHARDID());
            cache.addUploadShardCache(shardCache);
        } catch (Exception e) {
            LOG.error("", e);
        }
        return new VoidResp();
    }

    private void verify(UploadShardResp resp, byte[] key, int maxshardCount, int nodeid) throws ServiceException, NoSuchAlgorithmException, InvalidKeyException, SignatureException {
        if (resp.getSHARDID() >= maxshardCount) {
            throw new ServiceException(TOO_MANY_SHARDS);
        }
        ByteBuffer buf = ByteBuffer.allocate(48);
        buf.put(resp.getVHF());
        buf.putInt(resp.getSHARDID());
        buf.putInt(nodeid);
        buf.putLong(resp.getVBI());
        buf.flip();
        //if (!KeyStoreCoder.ecdsaVerify(buf.array(), resp.getUSERSIGN(), key)) {
        //LOG.info(resp.getSHARDID() + " getUSERSIGN " + Hex.encodeHexString(resp.getUSERSIGN()));
        // throw new ServiceException(INVALID_SIGNATURE);
        //} else {
        //    LOG.info(resp.getSHARDID() + " getUSERSIGN " + Hex.encodeHexString(resp.getUSERSIGN()));
        //}
    }

}
