package com.ytfs.service.servlet.s3;

import com.ytfs.service.dao.BucketCache;
import com.ytfs.service.dao.BucketMeta;
import com.ytfs.service.dao.User;
import com.ytfs.service.packet.s3.GetBucketReq;
import com.ytfs.service.packet.s3.GetBucketResp;
import com.ytfs.service.servlet.Handler;
import org.apache.log4j.Logger;

public class GetBuckettHandler extends Handler<GetBucketReq> {

    private static final Logger LOG = Logger.getLogger(GetBuckettHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        LOG.info("Delete bucket:" + user.getUserID() + "/" + request.getBucketName());
        BucketMeta meta = BucketCache.getBucket(user.getUserID(), request.getBucketName(),new byte[0]);
        GetBucketResp resp = new GetBucketResp();
        resp.setBucketName(meta.getBucketName());
        resp.setMeta(meta.getMeta());

        return resp;
    }

}
