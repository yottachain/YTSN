package com.ytfs.service.servlet.s3.v2;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.BucketAccessor;
import com.ytfs.service.dao.BucketCache;
import com.ytfs.service.dao.BucketMeta;
import com.ytfs.service.dao.User;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.servlet.Handler;
import org.apache.log4j.Logger;

import static com.ytfs.common.ServiceErrorCode.INVALID_BUCKET_NAME;
import com.ytfs.service.packet.s3.v2.UpdateBucketReqV2;

public class UpdateBucketHandler extends Handler<UpdateBucketReqV2> {

    private static final Logger LOG = Logger.getLogger(UpdateBucketHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser(request);
        if (user == null) {
            return new ServiceException(ServiceErrorCode.INVALID_SIGNATURE);
        }
        LOG.info("Update bucket:" + user.getUserID() + "/" + request.getBucketName());
        String name = request.getBucketName();
        name = name == null ? "" : name.trim();
        if (name.isEmpty() || name.length() > 20) {
            throw new ServiceException(INVALID_BUCKET_NAME);
        }
        BucketMeta meta = BucketCache.getBucket(user.getUserID(), request.getBucketName(), request.getMeta());
        BucketAccessor.updateBucketMeta(meta);
        return new VoidResp();
    }

}
