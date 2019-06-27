package com.ytfs.service.servlet.s3;

import com.ytfs.service.dao.BucketAccessor;
import com.ytfs.service.dao.BucketMeta;
import com.ytfs.service.dao.User;
import com.ytfs.service.packet.s3.GetBucketReq;
import com.ytfs.service.packet.s3.GetBucketResp;
import com.ytfs.service.servlet.Handler;
import org.apache.log4j.Logger;

public class GetBucketHandler extends Handler<GetBucketReq> {

    private static final Logger LOG = Logger.getLogger(GetBucketHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        LOG.info("GET bucket::::::" + user.getUserID() + "/" + request.getBucketName());
//        BucketMeta meta = BucketCache.getBucket(user.getUserID(), request.getBucketName(),new byte[0]);
        BucketMeta meta = BucketAccessor.getBucketMeta(user.getUserID(), request.getBucketName());
        GetBucketResp resp = new GetBucketResp();
        resp.setBucketName(meta.getBucketName());
        resp.setMeta(meta.getMeta());
        return resp;
    }

}
