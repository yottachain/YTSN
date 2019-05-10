package com.ytfs.service.servlet.s3;

import com.ytfs.service.dao.BucketCache;
import com.ytfs.service.dao.BucketMeta;
import com.ytfs.service.dao.FileAccessor;
import com.ytfs.service.dao.FileMeta;
import com.ytfs.service.dao.User;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.packet.s3.UploadFileReq;
import com.ytfs.service.servlet.Handler;
import static com.ytfs.service.utils.ServiceErrorCode.INVALID_BUCKET_NAME;
import static com.ytfs.service.utils.ServiceErrorCode.INVALID_UPLOAD_ID;
import com.ytfs.service.utils.ServiceException;
import org.apache.log4j.Logger;

public class UploadFileHandler extends Handler<UploadFileReq> {

    private static final Logger LOG = Logger.getLogger(UploadFileHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        LOG.info("Create object:" + user.getUserID() + "/" + request.getBucketname() + "/" + request.getFileName());
        BucketMeta meta = BucketCache.getBucket(user.getUserID(), request.getBucketname());
        if (meta == null) {
            throw new ServiceException(INVALID_BUCKET_NAME);
        }
        if (request.getVNU() == null) {
            throw new ServiceException(INVALID_UPLOAD_ID);
        }
        FileMeta filemeta = new FileMeta();
        filemeta.setBucketId(meta.getBucketId());
        filemeta.setFileName(request.getFileName());
        filemeta.setVNU(request.getVNU());
        FileAccessor.saveFileMeta(filemeta);
        return new VoidResp();
    }

}
