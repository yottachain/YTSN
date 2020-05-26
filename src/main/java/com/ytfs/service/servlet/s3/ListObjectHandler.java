package com.ytfs.service.servlet.s3;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.*;
import com.ytfs.service.packet.s3.ListObjectReq;
import com.ytfs.service.servlet.Handler;
import org.apache.log4j.Logger;

public class ListObjectHandler extends Handler<ListObjectReq> {

    private static final Logger LOG = Logger.getLogger(ListObjectHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        if (user == null) {
            return new ServiceException(ServiceErrorCode.NEED_LOGIN);
        }
        FileListCache cache = new FileListCache(request, user.getUserID());
        try {
            cache.list();
            return cache.getResult();
        } catch (Throwable e) {
            LOG.error("LIST object:" + user.getUserID() + "/" + request.getBucketName() + "/" + request.getPrefix() + ",ERR:" + e.getMessage());
            return e instanceof ServiceException ? (ServiceException) e : new ServiceException(ServiceErrorCode.SERVER_ERROR, e.getMessage());
        }
    }
}
