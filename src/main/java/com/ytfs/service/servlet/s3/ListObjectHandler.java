package com.ytfs.service.servlet.s3;

import com.ytfs.service.dao.*;
import com.ytfs.service.packet.s3.ListObjectReq;
import com.ytfs.service.packet.s3.ListObjectResp;
import com.ytfs.service.packet.s3.entities.FileMetaMsg;
import com.ytfs.service.servlet.Handler;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;

public class ListObjectHandler extends Handler<ListObjectReq> {

    private static final Logger LOG = Logger.getLogger(ListObjectHandler.class);

    @Override
    public Object handle() throws Throwable {
        try {
            User user = this.getUser();
            LOG.info("LIST object:" + user.getUserID());
            int limit = request.getLimit();
            String prefix = request.getPrefix();
            ObjectId nextVersionId = request.getNextVersionId();
            BucketMeta meta = BucketCache.getBucket(user.getUserID(), request.getBucketName(), null);
            String fileName = request.getFileName();
            List<FileMetaV2> fileMetaV2s = FileAccessorV2.listBucket(meta.getBucketId(), fileName, nextVersionId, prefix, limit);
            List<FileMetaMsg> fileMetaMsgs = new ArrayList<>();
            if (fileMetaV2s.size() > 0) {
                for (FileMetaV2 fileMetaV2 : fileMetaV2s) {
                    FileMetaMsg fileMetaMsg = new FileMetaMsg();
                    fileMetaMsg.setAcl(fileMetaV2.getAcl());
                    fileMetaMsg.setBucketId(fileMetaV2.getBucketId());
                    fileMetaMsg.setFileId(fileMetaV2.getFileId());
                    fileMetaMsg.setFileName(fileMetaV2.getFileName());
                    fileMetaMsg.setMeta(fileMetaV2.getMeta());
                    fileMetaMsg.setVersionId(fileMetaV2.getVersionId());
                    //LOG.info("IsLastest ===========" + fileMetaV2.isLatest());
                    fileMetaMsg.setLatest(fileMetaV2.isLatest());
                    fileMetaMsgs.add(fileMetaMsg);
                }
            }
            ListObjectResp resp = new ListObjectResp();
            resp.setFileMetaMsgList(fileMetaMsgs);
            LOG.info("ListObject return line:" + fileMetaV2s.size());
            return resp;
        } catch (Throwable e) {
            LOG.error("ListObject ERR:", e);
            throw e;
        }
    }

}
