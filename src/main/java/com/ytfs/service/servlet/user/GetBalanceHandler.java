package com.ytfs.service.servlet.user;

import com.ytfs.service.dao.ObjectAccessor;
import com.ytfs.service.dao.ObjectMeta;
import com.ytfs.service.dao.User;
import com.ytfs.service.eos.EOSClient;
import com.ytfs.service.packet.GetBalanceReq;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.servlet.Handler;
import com.ytfs.service.utils.ServiceErrorCode;
import com.ytfs.service.utils.ServiceException;
import org.apache.log4j.Logger;

public class GetBalanceHandler extends Handler<GetBalanceReq> {

    private static final Logger LOG = Logger.getLogger(GetBalanceHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        LOG.info("Get Balance:" + user.getUserID());
        boolean has = EOSClient.hasSpace(request.getLength(), request.getSignData(), request.getVNU());
        if (has) {
            ObjectMeta meta = new ObjectMeta(user.getUserID(), request.getVHW());
            meta.setLength(request.getLength());
            meta.setVNU(request.getVNU());
            meta.setNLINK(0);
            ObjectAccessor.insertOrUpdate(meta);
        } else {
            throw new ServiceException(ServiceErrorCode.NOT_ENOUGH_DHH);
        }
        return new VoidResp();
    }

}
