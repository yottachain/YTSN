package com.ytfs.service.servlet.s3;

import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.*;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.packet.s3.DeleteBucketReq;
import com.ytfs.service.servlet.Handler;
import org.apache.log4j.Logger;

import java.util.Map;

import static com.ytfs.common.ServiceErrorCode.BUCKET_NOT_EMPTY;
import static com.ytfs.common.ServiceErrorCode.INVALID_BUCKET_NAME;

public class DeleteBucketHandler extends Handler<DeleteBucketReq> {

    private static final Logger LOG = Logger.getLogger(DeleteBucketHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        LOG.info("Delete bucket:" + "/" + request.getBucketName());
        BucketMeta meta = BucketCache.getBucket(user.getUserID(), request.getBucketName(),new byte[0]);
        //meta is null,当前bucket不存在
        if(meta == null) {
            throw new ServiceException(INVALID_BUCKET_NAME);
        }
        //判断bucket下是否有文件,如果有文件则不允许删除bucket
        Map<String,byte[]> map = FileAccessor.listObjectByBucket(meta.getBucketId());
        LOG.info("map.size()=======" + map.size());
        if(map.size() > 0) {
            throw new ServiceException(BUCKET_NOT_EMPTY);
        }
        BucketAccessor.deleteBucketMeta(meta);
        return new VoidResp();
    }

}
