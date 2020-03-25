package com.ytfs.service.servlet.s3.v2;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.BucketAccessor;
import com.ytfs.service.dao.BucketMeta;
import com.ytfs.service.dao.User;
import com.ytfs.service.packet.s3.GetBucketResp;
import com.ytfs.service.packet.s3.v2.GetBucketReqV2;
import com.ytfs.service.servlet.Handler;
import org.apache.log4j.Logger;

public class GetBucketHandler extends Handler<GetBucketReqV2> {

    private static final Logger LOG = Logger.getLogger(GetBucketHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser(request);
        if (user == null) {
            return new ServiceException(ServiceErrorCode.INVALID_SIGNATURE);
        }
        LOG.info("GET bucket::::::" + user.getUserID() + "/" + request.getBucketName());
        BucketMeta meta = BucketAccessor.getBucketMeta(user.getUserID(), request.getBucketName());
        GetBucketResp resp = new GetBucketResp();
        resp.setBucketName(meta.getBucketName());
        resp.setMeta(meta.getMeta());
        return resp;
    }

}
