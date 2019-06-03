package com.ytfs.service.servlet.s3;

import com.ytfs.service.dao.BucketAccessor;
import com.ytfs.service.dao.User;
import com.ytfs.service.packet.s3.ListBucketReq;
import com.ytfs.service.packet.s3.ListBucketResp;
import com.ytfs.service.servlet.Handler;
import org.apache.log4j.Logger;

public class ListBucketHandler extends Handler<ListBucketReq> {

    private static final Logger LOG = Logger.getLogger(ListBucketHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        LOG.info("LIST bucket:" + user.getUserID());
        String[] names = BucketAccessor.listBucket(user.getUserID());
        ListBucketResp resp = new ListBucketResp();
        resp.setNames(names);
        return resp;
    }

}
