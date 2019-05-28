package com.ytfs.service.servlet.s3;

import com.ytfs.service.dao.BucketCache;
import com.ytfs.service.dao.BucketMeta;
import com.ytfs.service.dao.FileAccessor;
import com.ytfs.service.dao.User;
import com.ytfs.service.packet.s3.ListObjectReq;
import com.ytfs.service.packet.s3.ListObjectResp;
import com.ytfs.service.servlet.Handler;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import java.util.LinkedHashMap;
import java.util.Map;

public class ListObjectHandler extends Handler<ListObjectReq> {

    private static final Logger LOG = Logger.getLogger(ListObjectHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        LOG.info("LIST object:" + user.getUserID());
        int limit = request.getLimit();
        BucketMeta meta = BucketCache.getBucket(user.getUserID(), request.getBucketName(),null);
        String fileName = request.getFileName();
        Map<String,byte[]> map = new LinkedHashMap<>();
        String lastFileName = FileAccessor.listObjectByBucket(map, meta.getBucketId(), fileName, limit);
        ListObjectResp resp = new ListObjectResp();
        resp.setMap(map);
        resp.setFileName(lastFileName);
        return resp;
    }

}
