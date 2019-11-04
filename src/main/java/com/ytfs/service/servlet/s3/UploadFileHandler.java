package com.ytfs.service.servlet.s3;

import com.ytfs.service.dao.*;
import com.ytfs.service.servlet.Handler;
import static com.ytfs.common.ServiceErrorCode.INVALID_BUCKET_NAME;
import static com.ytfs.common.ServiceErrorCode.INVALID_UPLOAD_ID;
import com.ytfs.common.ServiceException;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.packet.s3.UploadFileReq;
import org.apache.log4j.Logger;

public class UploadFileHandler extends Handler<UploadFileReq> {

    private static final Logger LOG = Logger.getLogger(UploadFileHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
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
