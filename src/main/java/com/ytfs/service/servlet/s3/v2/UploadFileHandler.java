package com.ytfs.service.servlet.s3.v2;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.service.dao.*;
import com.ytfs.service.servlet.Handler;
import static com.ytfs.common.ServiceErrorCode.INVALID_BUCKET_NAME;
import static com.ytfs.common.ServiceErrorCode.INVALID_UPLOAD_ID;
import com.ytfs.common.ServiceException;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.packet.s3.v2.UploadFileReqV2;
import org.apache.log4j.Logger;

public class UploadFileHandler extends Handler<UploadFileReqV2> {

    private static final Logger LOG = Logger.getLogger(UploadFileHandler.class);

    @Override
    public int GetDoType() {
        return 1;
    }

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser(request);
        if (user == null) {
            return new ServiceException(ServiceErrorCode.INVALID_SIGNATURE);
        }
        LOG.info("Create object:" + user.getUserID() + "/" + request.getBucketname() + "/" + request.getFileName());
        BucketMeta meta = BucketCache.getBucket(user.getUserID(), request.getBucketname(), request.getMeta());
        if (meta == null) {
            throw new ServiceException(INVALID_BUCKET_NAME);
        }
        if (request.getVNU() == null) {
            throw new ServiceException(INVALID_UPLOAD_ID);
        }
        FileMetaV2 filemeta = new FileMetaV2();
        filemeta.setBucketId(meta.getBucketId());
        filemeta.setFileName(request.getFileName());
        filemeta.setMeta(request.getMeta());
        filemeta.setVersionId(request.getVNU());
        FileAccessorV2.saveFileMeta(user.getUserID(), filemeta);
        return new VoidResp();
    }

}
