package com.ytfs.service.servlet.s3;

import com.ytfs.service.dao.*;
import com.ytfs.service.packet.s3.ListObjectReq;
import com.ytfs.service.packet.s3.ListObjectResp;
import com.ytfs.service.servlet.Handler;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.List;

public class ListObjectHandler extends Handler<ListObjectReq> {

    private static final Logger LOG = Logger.getLogger(ListObjectHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        LOG.info("LIST object:" + user.getUserID());
        int limit = request.getLimit();
        String prefix = request.getPrefix();
        BucketMeta meta = BucketCache.getBucket(user.getUserID(), request.getBucketName(),null);
        String fileName = request.getFileName();

        List<FileMetaV2> fileMetaV2s = FileAccessorV2.listBucket(meta.getBucketId(), fileName, null,  prefix,  limit);
        ListObjectResp resp = new ListObjectResp();
        resp.setObjects(Collections.singletonList(fileMetaV2s));
        return resp;
    }

}
