package com.ytfs.service.servlet.s3;

import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.*;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.packet.s3.DeleteFileReq;
import com.ytfs.service.servlet.Handler;
import org.apache.log4j.Logger;

import static com.ytfs.common.ServiceErrorCode.INVALID_OBJECT_NAME;

public class DeleteFileHandler extends Handler<DeleteFileReq> {

    private static final Logger LOG = Logger.getLogger(DeleteFileHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        LOG.info("Delete object:" + "/" + request.getBucketName() + "/" + request.getFileName());
        BucketMeta meta = BucketCache.getBucket(user.getUserID(), request.getBucketName(),request.getMeta());

        FileMeta fileMeta = FileAccessor.getFileMeta(meta.getBucketId(),request.getFileName());
        if(fileMeta == null) {
            throw new ServiceException(INVALID_OBJECT_NAME);
        }

        FileAccessor.deleteFileMeta(fileMeta);
        return new VoidResp();
    }

}
