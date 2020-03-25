package com.ytfs.service.servlet.s3.v2;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.*;
import com.ytfs.service.packet.s3.GetObjectResp;
import com.ytfs.service.packet.s3.v2.GetObjectReqV2;
import com.ytfs.service.servlet.Handler;
import org.apache.log4j.Logger;

public class GetObjectHandler extends Handler<GetObjectReqV2> {

    private static final Logger LOG = Logger.getLogger(GetObjectHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser(request);
        if (user == null) {
            return new ServiceException(ServiceErrorCode.INVALID_SIGNATURE);
        }
        LOG.info("Get object:" + user.getUserID() + "/" + request.getBucketName()+"/"+request.getFileName());
        BucketMeta meta = BucketCache.getBucket(user.getUserID(), request.getBucketName(), new byte[0]);
        FileMetaV2 fileMeta = FileAccessorV2.getFileMeta(user.getUserID(), meta.getBucketId(), request.getFileName());
        GetObjectResp resp = new GetObjectResp();
        if (fileMeta != null) {
            resp.setFileName(fileMeta.getFileName());
            resp.setObjectId(fileMeta.getFileId());
        } else {
            resp.setFileName(null);
            resp.setObjectId(null);
        }
        return resp;
    }

}
