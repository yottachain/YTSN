package com.ytfs.service.servlet.s3;

import com.ytfs.service.dao.BucketAccessor;
import com.ytfs.service.dao.BucketMeta;
import com.ytfs.service.dao.User;
import com.ytfs.service.servlet.Handler;
import static com.ytfs.common.ServiceErrorCode.INVALID_BUCKET_NAME;
import com.ytfs.common.ServiceException;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.packet.s3.CreateBucketReq;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

public class CreateBucketHandler extends Handler<CreateBucketReq> {

    private static final Logger LOG = Logger.getLogger(CreateBucketHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        LOG.info("Crate bucket:" + user.getUserID() + "/" + request.getBucketName());
        String name = request.getBucketName();
        name = name == null ? "" : name.trim();
        if (name.isEmpty() || name.length() > 20) {
            throw new ServiceException(INVALID_BUCKET_NAME);
        }
        BucketMeta meta = new BucketMeta(user.getUserID(), new ObjectId(), name);
        BucketAccessor.saveBucketMeta(meta);
        return new VoidResp();
    }

}
