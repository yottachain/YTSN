package com.ytfs.service.servlet;

import com.ytfs.service.dao.BucketAccessor;
import com.ytfs.service.dao.BucketCache;
import com.ytfs.service.dao.BucketMeta;
import com.ytfs.service.dao.FileAccessor;
import com.ytfs.service.dao.FileMeta;
import com.ytfs.service.dao.ObjectAccessor;
import com.ytfs.service.dao.ObjectMeta;
import com.ytfs.service.dao.User;
import com.ytfs.service.packet.s3.CreateBucketReq;
import com.ytfs.service.packet.s3.DownloadFileReq;
import com.ytfs.service.packet.DownloadObjectInitResp;
import com.ytfs.service.packet.s3.ListBucketReq;
import com.ytfs.service.packet.s3.ListBucketResp;
import static com.ytfs.service.utils.ServiceErrorCode.INVALID_BUCKET_NAME;
import static com.ytfs.service.utils.ServiceErrorCode.INVALID_OBJECT_NAME;
import static com.ytfs.service.utils.ServiceErrorCode.INVALID_UPLOAD_ID;
import com.ytfs.service.utils.ServiceException;
import com.ytfs.service.packet.s3.UploadFileReq;
import com.ytfs.service.packet.VoidResp;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

public class FileMetaHandler {

    private static final Logger LOG = Logger.getLogger(FileMetaHandler.class);

    static VoidResp writeFileMeta(UploadFileReq req, User user) throws ServiceException, Throwable {
        LOG.info("Create object:" + user.getUserID() + "/" + req.getBucketname() + "/" + req.getFileName());
        BucketMeta meta = BucketCache.getBucket(user.getUserID(), req.getBucketname());
        if (meta == null) {
            throw new ServiceException(INVALID_BUCKET_NAME);
        }
        if (req.getVNU() == null) {
            throw new ServiceException(INVALID_UPLOAD_ID);
        }
        FileMeta filemeta = new FileMeta();
        filemeta.setBucketId(meta.getBucketId());
        filemeta.setFileName(req.getFileName());
        filemeta.setVNU(req.getVNU());
        FileAccessor.saveFileMeta(filemeta);
        return new VoidResp();
    }

    static DownloadObjectInitResp readFileMeta(DownloadFileReq req, User user) throws ServiceException, Throwable {
        LOG.info("Read object:" + user.getUserID() + "/" + req.getBucketname() + "/" + req.getFileName());
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
        LOG.info("Crate bucket:" + user.getUserID() + "/" + req.getBucketName());
        String name = req.getBucketName();
        name = name == null ? "" : name.trim();
        if (name.isEmpty() || name.length() > 20) {
            throw new ServiceException(INVALID_BUCKET_NAME);
        }
        BucketMeta meta = new BucketMeta(user.getUserID(), new ObjectId(), name);
        BucketAccessor.saveBucketMeta(meta);
        return new VoidResp();
    }

    static ListBucketResp listBucket(ListBucketReq req, User user) throws ServiceException, Throwable {
        LOG.info("LIST bucket:" + user.getUserID());
        String[] names = BucketAccessor.listBucket(user.getUserID());
        ListBucketResp resp = new ListBucketResp();
        resp.setNames(names);
        return resp;
    }
}
