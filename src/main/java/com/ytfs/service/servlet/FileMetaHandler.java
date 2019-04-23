package com.ytfs.service.servlet;

import com.ytfs.service.dao.BucketCache;
import com.ytfs.service.dao.BucketMeta;
import com.ytfs.service.dao.FileAccessor;
import com.ytfs.service.dao.FileMeta;
import com.ytfs.service.dao.ObjectAccessor;
import com.ytfs.service.dao.ObjectMeta;
import com.ytfs.service.dao.User;
import com.ytfs.service.packet.CreateBucketReq;
import com.ytfs.service.packet.DownloadFileReq;
import com.ytfs.service.packet.DownloadObjectInitResp;
import static com.ytfs.service.packet.ServiceErrorCode.INVALID_BUCKET_NAME;
import static com.ytfs.service.packet.ServiceErrorCode.INVALID_OBJECT_NAME;
import com.ytfs.service.packet.ServiceException;
import com.ytfs.service.packet.UploadFileReq;
import com.ytfs.service.packet.VoidResp;

public class FileMetaHandler {

    static VoidResp writeFileMeta(UploadFileReq req, User user) throws ServiceException, Throwable {
        BucketMeta meta = BucketCache.getBucket(user.getUserID(), req.getBucketname());
        if (meta == null) {
            throw new ServiceException(INVALID_BUCKET_NAME);
        }
        FileMeta filemeta = new FileMeta();
        filemeta.setBucketId(meta.getBucketId());
        filemeta.setFileName(req.getFileName());
        filemeta.setVNU(req.getVNU());
        FileAccessor.saveFileMeta(filemeta);
        return new VoidResp();
    }

    static DownloadObjectInitResp readFileMeta(DownloadFileReq req, User user) throws ServiceException, Throwable {
        BucketMeta meta = BucketCache.getBucket(user.getUserID(), req.getBucketname());
        if (meta == null) {
            throw new ServiceException(INVALID_BUCKET_NAME);
        }
        FileMeta fmeta = FileAccessor.getFileMeta(meta.getBucketId(), req.getFileName());
        if (fmeta == null) {
            throw new ServiceException(INVALID_OBJECT_NAME);
        }
        ObjectMeta ometa = ObjectAccessor.getObject(fmeta.getVNU());
        DownloadObjectInitResp resp = new DownloadObjectInitResp();
        resp.setRefers(ometa.getBlocks());
        resp.setLength(ometa.getLength());
        return resp;
    }

    static VoidResp createBucket(CreateBucketReq req, User user) throws ServiceException, Throwable {

        return null;

    }
}
