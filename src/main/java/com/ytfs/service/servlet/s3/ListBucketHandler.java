package com.ytfs.service.servlet.s3;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.BucketAccessor;
import com.ytfs.service.dao.FileListCache;
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
        if (user == null) {
            return new ServiceException(ServiceErrorCode.NEED_LOGIN);
        }
        String[] names=FileListCache.getBucketCache(String.valueOf(user.getUserID()));
        if(names==null){
            LOG.debug("LIST bucket:" + user.getUserID());       
            names = BucketAccessor.listBucket(user.getUserID());
            FileListCache.putBucketCache(String.valueOf(user.getUserID()), names);
        }       
        ListBucketResp resp = new ListBucketResp();
        resp.setNames(names);   
        return resp;
    }

}
