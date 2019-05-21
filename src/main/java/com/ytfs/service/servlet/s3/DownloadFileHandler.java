package com.ytfs.service.servlet.s3;

import com.ytfs.service.dao.BucketCache;
import com.ytfs.service.dao.BucketMeta;
import com.ytfs.service.dao.FileAccessor;
import com.ytfs.service.dao.FileMeta;
import com.ytfs.service.dao.ObjectAccessor;
import com.ytfs.service.dao.ObjectMeta;
import com.ytfs.service.servlet.Handler;
import static com.ytfs.common.ServiceErrorCode.INVALID_BUCKET_NAME;
import static com.ytfs.common.ServiceErrorCode.INVALID_OBJECT_NAME;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.User;
import com.ytfs.service.packet.DownloadObjectInitResp;
import com.ytfs.service.packet.s3.DownloadFileReq;
import org.apache.log4j.Logger;

public class DownloadFileHandler extends Handler<DownloadFileReq> {

    private static final Logger LOG = Logger.getLogger(DownloadFileHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        LOG.info("Read object:" + user.getUserID() + "/" + request.getBucketname() + "/" + request.getFileName());
        BucketMeta meta = BucketCache.getBucket(user.getUserID(), request.getBucketname(),new byte[0]);
        if (meta == null) {
            throw new ServiceException(INVALID_BUCKET_NAME);
        }
        FileMeta fmeta = FileAccessor.getFileMeta(meta.getBucketId(), request.getFileName());
        LOG.info("fmeta==================fileName=====" + request.getFileName());
        LOG.info("fmeta==================bucketName=====" + request.getBucketname());
        if (fmeta == null) {
            throw new ServiceException(INVALID_OBJECT_NAME);
        }
        ObjectMeta ometa = ObjectAccessor.getObject(fmeta.getVNU());
        DownloadObjectInitResp resp = new DownloadObjectInitResp();
        resp.setRefers(ometa.getBlocks());
        resp.setLength(ometa.getLength());
        return resp;
    }

}
