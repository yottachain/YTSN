package com.ytfs.service.servlet.s3;

import com.ytfs.service.dao.*;
import com.ytfs.service.servlet.Handler;
import static com.ytfs.common.ServiceErrorCode.INVALID_BUCKET_NAME;
import static com.ytfs.common.ServiceErrorCode.INVALID_OBJECT_NAME;
import com.ytfs.common.ServiceException;
import com.ytfs.service.packet.DownloadObjectInitResp;
import com.ytfs.service.packet.s3.DownloadFileReq;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

public class DownloadFileHandler extends Handler<DownloadFileReq> {

    private static final Logger LOG = Logger.getLogger(DownloadFileHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        LOG.info("Read object:" + user.getUserID() + "/" + request.getBucketname() + "/" + request.getFileName());
        ObjectId versionId = request.getVersionId();
        BucketMeta meta = BucketCache.getBucket(user.getUserID(), request.getBucketname(), new byte[0]);
        if (meta == null) {
            throw new ServiceException(INVALID_BUCKET_NAME);
        }
        FileMetaV2 fmeta = FileAccessorV2.getFileMeta(meta.getBucketId(), request.getFileName(), versionId);
        if (fmeta == null) {
            throw new ServiceException(INVALID_OBJECT_NAME);
        }
        ObjectMeta ometa = ObjectAccessor.getObject(fmeta.getVersionId());
        DownloadObjectInitResp resp = new DownloadObjectInitResp();
        resp.setOldRefers(ometa.getBlocks());
        resp.setRefers(ometa.getBlockList());
        resp.setLength(ometa.getLength());
        return resp;
    }

}
