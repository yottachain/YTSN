package com.ytfs.service.servlet.s3;

import com.ytfs.service.dao.*;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.packet.s3.DeleteFileReq;
import com.ytfs.service.servlet.Handler;
import org.apache.log4j.Logger;

public class DeleteFileHandler extends Handler<DeleteFileReq> {

    private static final Logger LOG = Logger.getLogger(DeleteFileHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        LOG.info("Delete object:" + user.getUserID() + "/" + request.getBucketname() + "/" + request.getFileName());
        BucketMeta meta = BucketCache.getBucket(user.getUserID(), request.getBucketname(),request.getMeta());

        FileMeta filemeta = new FileMeta();
        filemeta.setBucketId(meta.getBucketId());
        filemeta.setFileName(request.getFileName());
        FileAccessor.deleteFileMeta(filemeta);
        return new VoidResp();
    }

}
