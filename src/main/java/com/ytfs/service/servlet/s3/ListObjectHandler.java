package com.ytfs.service.servlet.s3;

import com.ytfs.service.dao.*;
import com.ytfs.service.packet.s3.ListObjectReq;
import com.ytfs.service.packet.s3.ListObjectResp;
import com.ytfs.service.servlet.Handler;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import java.util.Map;
import java.util.TreeMap;

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
        Map<String,byte[]> map = new TreeMap<>();
//        ObjectId objectId = FileAccessor.listObjectByBucket(map, meta.getBucketId(), startId, limit);
        Map<ObjectId,String> lastMap = FileAccessor.listObjectByBucket(map, meta.getBucketId(), startId, limit);

        LOG.info("map.size()==================="+map.size());
        ListObjectResp resp = new ListObjectResp();
        resp.setMap(map);
        if(lastMap.size() == 1) {
            for(Map.Entry<ObjectId,String> entry : lastMap.entrySet()) {
                resp.setObjectId(entry.getKey());
                resp.setFileName(entry.getValue());
            }
        }
        return resp;
    }

}
