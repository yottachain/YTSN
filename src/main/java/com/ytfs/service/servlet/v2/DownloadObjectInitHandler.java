package com.ytfs.service.servlet.v2;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.ObjectAccessor;
import com.ytfs.service.dao.ObjectMeta;
import com.ytfs.service.dao.User;
import com.ytfs.service.packet.user.DownloadObjectInitResp;
import com.ytfs.service.packet.v2.DownloadObjectInitReqV2;
import com.ytfs.service.servlet.Handler;
import org.apache.log4j.Logger;

public class DownloadObjectInitHandler extends Handler<DownloadObjectInitReqV2> {

    private static final Logger LOG = Logger.getLogger(DownloadObjectInitHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser(request);
        if (user == null) {
            return new ServiceException(ServiceErrorCode.INVALID_SIGNATURE);
        }
        int userid = user.getUserID();
        ObjectMeta meta = ObjectAccessor.getObject(userid, request.getVHW());
        LOG.info("Download object:" + userid + "/" + meta.getVNU());
        DownloadObjectInitResp resp = new DownloadObjectInitResp();
        resp.setRefers(meta.getBlockList());
        resp.setLength(meta.getLength());
        return resp;
    }

}
