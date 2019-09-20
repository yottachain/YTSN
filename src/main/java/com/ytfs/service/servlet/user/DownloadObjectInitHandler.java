package com.ytfs.service.servlet.user;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.ObjectAccessor;
import com.ytfs.service.dao.ObjectMeta;
import com.ytfs.service.dao.User;
import com.ytfs.service.packet.user.DownloadObjectInitReq;
import com.ytfs.service.packet.user.DownloadObjectInitResp;
import com.ytfs.service.servlet.Handler;
import org.apache.log4j.Logger;

public class DownloadObjectInitHandler extends Handler<DownloadObjectInitReq> {

    private static final Logger LOG = Logger.getLogger(DownloadObjectInitHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        if (user == null) {
            return new ServiceException(ServiceErrorCode.NEED_LOGIN);
        }
        int userid = user.getUserID();
        ObjectMeta meta = ObjectAccessor.getObject(userid, request.getVHW());
        LOG.info("Download object:" + userid + "/" + meta.getVNU());
        DownloadObjectInitResp resp = new DownloadObjectInitResp();
        resp.setOldRefers(meta.getBlocks());
        resp.setRefers(meta.getBlockList());
        resp.setLength(meta.getLength());
        return resp;
    }

}
