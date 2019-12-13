package com.ytfs.service.servlet.s3;

import com.ytfs.common.SerializationUtil;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.*;
import com.ytfs.service.packet.s3.CopyObjectReq;
import com.ytfs.service.packet.s3.CopyObjectResp;
import com.ytfs.service.servlet.Handler;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.Map;

import static com.ytfs.common.ServiceErrorCode.INVALID_BUCKET_NAME;

public class CopyObjectHandler extends Handler<CopyObjectReq> {

    private static final Logger LOG = Logger.getLogger(CopyObjectHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        LOG.info("Copy object:" + user.getUserID() + "/" + request.getSrcBucket() + "/" + request.getSrcObjectKey());
        BucketMeta meta = BucketCache.getBucket(user.getUserID(), request.getSrcBucket(),request.getMeta());
        BucketMeta destMeta = BucketCache.getBucket(user.getUserID(), request.getDestBucket(),request.getMeta());
        if (meta == null || destMeta == null) {
            throw new ServiceException(INVALID_BUCKET_NAME);
        }
        //根据bucket object获取文件的全部信息
        FileMetaV2 metaV2 = FileAccessorV2.getFileMeta(user.getUserID(),meta.getBucketId(),request.getSrcObjectKey());

        Map<String, String> header = SerializationUtil.deserializeMap(metaV2.getMeta());
        Date date = new Date();
        header.put("x-amz-date", date.getTime()+"");
        header.put("date",date.getTime()+"");
        byte[] bs = SerializationUtil.serializeMap(header);
        FileMetaV2 filemeta = new FileMetaV2();
        filemeta.setBucketId(destMeta.getBucketId());
        filemeta.setFileName(request.getDestObjectKey());
        filemeta.setMeta(bs);
        filemeta.setVersionId(metaV2.getVersionId());
        FileAccessorV2.saveFileMeta(user.getUserID(),filemeta);
        CopyObjectResp resp = new CopyObjectResp();
        resp.setBucketId(filemeta.getBucketId());
        resp.setBucketName(filemeta.getFileName());
        resp.setMeta(filemeta.getMeta());
        resp.setVersionId(filemeta.getVersionId());
        return resp;
    }

}
