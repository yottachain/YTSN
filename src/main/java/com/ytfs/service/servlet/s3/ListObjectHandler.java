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

import java.util.HashMap;
import java.util.Map;

public class ListObjectHandler extends Handler<ListObjectReq> {

    private static final Logger LOG = Logger.getLogger(ListObjectHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        LOG.info("LIST object:" + user.getUserID());
        ObjectId startId = request.getStartId();
        int limit = request.getLimit();
        BucketMeta meta = BucketCache.getBucket(user.getUserID(), request.getBucketName(),null);
        LOG.info("bucketId ======="+meta.getBucketId());
        Map<String,byte[]> map = new HashMap<>();
        ObjectId objectId = FileAccessor.listObjectByBucket(map, meta.getBucketId(), startId, limit);
        LOG.info("map.size()==================="+map.size());
        ListObjectResp resp = new ListObjectResp();
        resp.setMap(map);
        resp.setObjectId(objectId);
        return resp;
    }

}
