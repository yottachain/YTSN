package com.ytfs.service.servlet.s3.v2;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.*;
import com.ytfs.service.packet.s3.ListObjectResp;
import com.ytfs.service.packet.s3.ListObjectRespV2;
import com.ytfs.service.packet.s3.entities.FileMetaMsg;
import com.ytfs.service.packet.s3.v2.ListObjectReqV2;
import com.ytfs.service.servlet.Handler;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;

public class ListObjectHandler extends Handler<ListObjectReqV2> {

    private static final Logger LOG = Logger.getLogger(ListObjectHandler.class);

    @Override
    public Object handle() throws Throwable {
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
        LOG.info("LIST object:" + user.getUserID() + "/" + request.getPrefix());
        int limit = request.getLimit();
        String prefix = request.getPrefix();
        ObjectId nextVersionId = request.getNextVersionId();
        BucketMeta meta = BucketCache.getBucket(user.getUserID(), request.getBucketName(), null);
        String fileName = request.getFileName();
        //List<FileMetaV2> fileMetaV2s = FileListCache.listBucket(this.getPublicKey(), user.getUserID(), meta.getBucketId(), fileName, nextVersionId, prefix, limit);
        List<FileMetaV2> fileMetaV2s = FileAccessorV2.listBucket(user.getUserID(), meta.getBucketId(), fileName, nextVersionId, prefix, limit);
        List<FileMetaMsg> fileMetaMsgs = new ArrayList<>();
        if (!fileMetaV2s.isEmpty()) {
            for (FileMetaV2 fileMetaV2 : fileMetaV2s) {
                FileMetaMsg fileMetaMsg = new FileMetaMsg();
                fileMetaMsg.setAcl(fileMetaV2.getAcl());
                fileMetaMsg.setBucketId(fileMetaV2.getBucketId());
                fileMetaMsg.setFileId(fileMetaV2.getFileId());
                fileMetaMsg.setFileName(fileMetaV2.getFileName());
                fileMetaMsg.setMeta(fileMetaV2.getMeta());
                fileMetaMsg.setVersionId(fileMetaV2.getVersionId());
                fileMetaMsg.setLatest(fileMetaV2.isLatest());
                fileMetaMsgs.add(fileMetaMsg);
            }
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

}
