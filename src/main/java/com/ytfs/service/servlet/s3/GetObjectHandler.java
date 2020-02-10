package com.ytfs.service.servlet.s3;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.*;
import com.ytfs.service.packet.s3.GetBucketResp;
import com.ytfs.service.packet.s3.GetObjectReq;
import com.ytfs.service.packet.s3.GetObjectResp;
import com.ytfs.service.servlet.Handler;
import org.apache.log4j.Logger;

public class GetObjectHandler extends Handler<GetObjectReq> {

    private static final Logger LOG = Logger.getLogger(GetObjectHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        if (user == null) {
            return new ServiceException(ServiceErrorCode.NEED_LOGIN);
        }
        LOG.info("Delete bucket:" + user.getUserID() + "/" + request.getBucketName());
        BucketMeta meta = BucketCache.getBucket(user.getUserID(), request.getBucketName(), new byte[0]);
//        FileMeta fileMeta = FileAccessor.getFileMeta(meta.getBucketId(),request.getFileName());
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
