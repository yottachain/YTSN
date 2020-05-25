package com.ytfs.service.servlet.s3.v2;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import static com.ytfs.common.conf.ServerConfig.lsCacheExpireTime;
import com.ytfs.service.dao.*;
import com.ytfs.service.packet.s3.ListObjectResp;
import com.ytfs.service.packet.s3.ListObjectRespV2;
import com.ytfs.service.packet.s3.entities.FileMetaMsg;
import com.ytfs.service.packet.s3.v2.ListObjectReqV2;
import com.ytfs.service.servlet.Handler;
import static com.ytfs.service.servlet.s3.ListObjectHandler.lstime;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

public class ListObjectHandler extends Handler<ListObjectReqV2> {

    private static final Logger LOG = Logger.getLogger(ListObjectHandler.class);

    public Object handleV0() throws Throwable {
        User user = this.getUser(request);
        if (user == null) {
            return new ServiceException(ServiceErrorCode.INVALID_SIGNATURE);
        }
        String key = request.getHashCode(user.getUserID());
        Object obj = FileListCache.getL1Cache(key);
        if (obj != null) {
            LOG.info("LIST object:" + user.getUserID() + "/" + key + "/" + request.getPrefix() + ",return from L1 cache.");
            return obj;
        }

        if (lstime.containsKey(user.getUserID())) {
            if (System.currentTimeMillis() - lstime.get(user.getUserID()) > 1000 * lsCacheExpireTime) {
                lstime.put(user.getUserID(), System.currentTimeMillis());
            } else {
                LOG.info("LIST object:" + user.getUserID() + "/" + request.getPrefix() + " ERR:TOO_MANY_CURSOR");
                return new ServiceException(ServiceErrorCode.TOO_MANY_CURSOR);
            }
        } else {
            lstime.put(user.getUserID(), System.currentTimeMillis());
        }
        LOG.info("LIST object:" + user.getUserID() + "/" + request.getPrefix());

        int limit = request.getLimit();
        String prefix = request.getPrefix();
        ObjectId nextVersionId = request.getNextVersionId();
        BucketMeta meta = BucketCache.getBucket(user.getUserID(), request.getBucketName(), null);
        String fileName = request.getFileName();
        List<FileMetaV2> fileMetaV2s = FileAccessorV2.listBucket(user.getUserID(), meta.getBucketId(), fileName, nextVersionId, prefix, limit);
        List<FileMetaMsg> fileMetaMsgs = new ArrayList<>();
        if (!fileMetaV2s.isEmpty()) {
            fileMetaV2s.stream().map((fileMetaV2) -> {
                FileMetaMsg fileMetaMsg = new FileMetaMsg();
                fileMetaMsg.setAcl(fileMetaV2.getAcl());
                fileMetaMsg.setBucketId(fileMetaV2.getBucketId());
                fileMetaMsg.setFileId(fileMetaV2.getFileId());
                fileMetaMsg.setFileName(fileMetaV2.getFileName());
                fileMetaMsg.setMeta(fileMetaV2.getMeta());
                fileMetaMsg.setVersionId(fileMetaV2.getVersionId());
                fileMetaMsg.setLatest(fileMetaV2.isLatest());
                return fileMetaMsg;
            }).forEachOrdered((fileMetaMsg) -> {
                fileMetaMsgs.add(fileMetaMsg);
            });
        }
        if (request.isCompress()) {
            ListObjectRespV2 resp = new ListObjectRespV2();
            resp.setFileMetaMsgList(fileMetaMsgs);
            if (!fileMetaMsgs.isEmpty()) {
                FileListCache.putL1Cache(key, resp);
            } else {
                LOG.info("No result:" + user.getUserID() + "/" + request.getPrefix());
            }
            return resp;
        } else {
            ListObjectResp resp = new ListObjectResp();
            resp.setFileMetaMsgList(fileMetaMsgs);
            if (!fileMetaMsgs.isEmpty()) {
                FileListCache.putL1Cache(key, resp);
            } else {
                LOG.info("No result:" + user.getUserID() + "/" + request.getPrefix());
            }
            return resp;
        }
    }

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser(request);
        if (user == null) {
            return new ServiceException(ServiceErrorCode.INVALID_SIGNATURE);
        }
        String key = request.getHashCode(user.getUserID());
        Object obj = FileListCache.getL1Cache(key);
        if (obj != null) {
            LOG.info("LIST object:" + user.getUserID() + "/" + key + "/" + request.getPrefix() + ",return from L1 cache:" + FileListCache.getL1Cache().size());
            return obj;
        }
        if (lstime.containsKey(user.getUserID())) {
            if (System.currentTimeMillis() - lstime.get(user.getUserID()) > 1000 * lsCacheExpireTime) {
                lstime.put(user.getUserID(), System.currentTimeMillis());
            } else {
                LOG.info("LIST object:" + user.getUserID() + "/" + request.getPrefix() + " ERR:TOO_MANY_CURSOR");
                return new ServiceException(ServiceErrorCode.TOO_MANY_CURSOR);
            }
        } else {
            lstime.put(user.getUserID(), System.currentTimeMillis());
        }
        LOG.info("LIST object:" + user.getUserID() + "/" + request.getPrefix());
        BucketMeta meta = BucketCache.getBucket(user.getUserID(), request.getBucketName(), null);
        try {
            FileListCache cache = new FileListCache(request);
            cache.listBucket(meta.getBucketId(), request.getBucketName(), user.getUserID(), key);
            return cache.getResult();
        } catch (Throwable e) {
            LOG.error("LIST object:" + user.getUserID() + "/" + key + "/" + request.getPrefix() + ",ERR:" + e.getMessage());
            return new ServiceException(ServiceErrorCode.SERVER_ERROR, e.getMessage());
        }
    }

}
